package main

import (
	"embed"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/els0r/telemetry/logging"
	"github.com/fako1024/fhttpc"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/common/model"
)

//go:embed testdata/alertmanager.test.yaml
var testdataFS embed.FS

const (
	nInstances         = 3
	nAlerts            = 5000
	nRestarts          = 3
	statusInfoInterval = 5 * time.Second

	binary = "./alertmanager"
)

var (
	log            *logging.L
	retryIntervals = fhttpc.Intervals{100 * time.Millisecond, 500 * time.Millisecond, time.Second}

	// PROD:
	// --config.file=/etc/alertmanager/config_out/alertmanager.env.yaml --storage.path=/alertmanager --data.retention=2160h
	// --cluster.listen-address=[10.32.16.96]:9094 --web.listen-address=:9093 --web.external-url=https://alertmgr.osdp.open.ch
	// --web.route-prefix=/ --cluster.pushpull-interval=5s --cluster.peer=alertmanager-osdp-monitoring-alertmanager-0.alertmanager-operated:9094
	// --cluster.peer=alertmanager-osdp-monitoring-alertmanager-1.alertmanager-operated:9094 --cluster.peer=alertmanager-osdp-monitoring-alertmanager-2.alertmanager-operated:9094
	// --cluster.reconnect-timeout=5m --web.config.file=/etc/alertmanager/web_config/web-config.yaml
	baseAMArgs = []string{
		"--data.retention=1h",
		"--web.route-prefix=/",
		"--cluster.pushpull-interval=5s",
		"--cluster.reconnect-timeout=15s",
	}
)

func TestAlertSequence(t *testing.T) {

	log.Infof("starting %d Alertmanager instances, waiting for gossip to settle...", nInstances)

	tempDir := t.TempDir()
	if err := prepareConfig(tempDir); err != nil {
		t.Fatalf("failed to prepare config: %v", err)
	}

	webhookConsumer := newWebhookConsumer()
	go webhookConsumer.ListenAndServe(":10025")

	ams := make(AMInstances, nInstances)
	for i := range nInstances {
		ams[i] = NewAMInstance(constructArgs(tempDir, nInstances, i))
	}

	err := ams.Start()
	if err != nil {
		t.Fatalf("failed to start Alertmanager instances: %v", err)
	}

	for {
		time.Sleep(1 * time.Second)
		healthy, ready, gossipSettled := ams.Health() == nil, ams.Ready() == nil, ams.GossipSettled()
		if healthy && ready && gossipSettled {
			break
		}
	}
	log.Infof("all instances ready / healthy - Sending %d alerts...", nAlerts)
	go trackAMs(ams, webhookConsumer)

	///////////////////////////////////////////////////////////////////////////////////////////////////
	// Choose your preferred method of sending alerts:
	sendAlertsAllInstances(t, ams, nAlerts) // default / recommended method
	// sendAlertsSingleInstance(t, ams[0], nAlerts)
	// sendAlertsSingleInstanceRR(t, ams, nAlerts)
	//////////////////////////////////////////////////////////////////////////////////////////////////

	log.Infof("sent %d alerts, waiting for state to settle...", nAlerts)
	for {
		time.Sleep(1 * time.Second)
		nNotifications := webhookConsumer.GetNNotifications()
		if nAlerts == nNotifications {
			log.Infof("alerts / notifications equalized (%d/%d), continuing...", nAlerts, nNotifications)
			break
		}
	}

	for i := range nRestarts {
		log.Infof("restarting Alertmanager instances (iteration %d/%d)...", i+1, nRestarts)
		wg := &sync.WaitGroup{}
		for k := range nInstances {
			wg.Add(1)
			go func(j int) {
				if err := ams[j].Restart(); err != nil {
					log.Errorf("error restarting Alertmanager instance: %v", err)
				}
				wg.Done()
			}(k)
		}
		wg.Wait()

		// Wait until all instances are ready / healthy again
		ams.WaitReadyAndHealthy()

		// Wait a moment to see if any notifications are re-sent after  the restart
		time.Sleep(30 * time.Second)
	}

	err = ams.Stop()
	if err != nil {
		t.Fatalf("failed to stop Alertmanager instances: %v", err)
	}

	for i := range nInstances {
		_ = os.WriteFile(fmt.Sprintf("./am%d.log", i), []byte(ams[i].Logs()), 0600)
	}
}

func sendAlertsSingleInstance(t *testing.T, am *AMInstance, nAlerts int) int {
	for i := range nAlerts {
		if err := am.SendAlert(types.Alert{
			Alert: model.Alert{
				Labels: model.LabelSet{
					"alertname": model.LabelValue(fmt.Sprintf("LatencyHigh_%d", i)),
					"cluster":   "test-cluster",
					"service":   "foo1",
					"severity":  "critical",
				},
				Annotations: model.LabelSet{
					"summary": "High latency detected",
					"desc":    "Latency is above threshold",
				},
				StartsAt: time.Now(),
				EndsAt:   time.Now().Add(1 * time.Hour),
			},
		}); err != nil {
			t.Fatalf("error sending alert: %v", err)
		}
	}

	return nAlerts
}

func sendAlertsAllInstances(t *testing.T, ams AMInstances, nAlerts int) int {
	for i := range nAlerts {
		for j := range nInstances {
			if err := ams[j].SendAlert(types.Alert{
				Alert: model.Alert{
					Labels: model.LabelSet{
						"alertname": model.LabelValue(fmt.Sprintf("LatencyHigh_%d", i)),
						"cluster":   "test-cluster",
						"service":   "foo1",
						"severity":  "critical",
					},
					Annotations: model.LabelSet{
						"summary": "High latency detected",
						"desc":    "Latency is above threshold",
					},
					StartsAt: time.Now(),
					EndsAt:   time.Now().Add(1 * time.Hour),
				},
			}); err != nil {
				t.Fatalf("error sending alert: %v", err)
			}
		}
	}

	return nAlerts
}

func sendAlertsSingleInstanceRR(t *testing.T, ams AMInstances, nAlerts int) int {
	nExpected := 0
	for i := range nAlerts {
		j1, j2 := i%nInstances, (i+1)%nInstances
		for _, j := range []int{j1, j2} {
			if j == 0 {
				nExpected++
			}
			if err := ams[j].SendAlert(types.Alert{
				Alert: model.Alert{
					Labels: model.LabelSet{
						"alertname": model.LabelValue(fmt.Sprintf("LatencyHigh_%d", i)),
						"cluster":   "test-cluster",
						"service":   "foo1",
						"severity":  "critical",
					},
					Annotations: model.LabelSet{
						"summary": "High latency detected",
						"desc":    "Latency is above threshold",
					},
					StartsAt: time.Now(),
					EndsAt:   time.Now().Add(1 * time.Hour),
				},
			}); err != nil {
				t.Fatalf("error sending alert: %v", err)
			}
		}
	}

	return nExpected
}

func trackAMs(ams AMInstances, wc *webhookConsumer) {
	curAlerts := make([]int, nInstances)
	for {
		time.Sleep(statusInfoInterval)

		healthy, ready := ams.Health() == nil, ams.Ready() == nil
		if !healthy || !ready {
			log.Warnf("alertmanager instances not healthy or ready (healthy: %v, ready: %v)", healthy, ready)
			continue
		}

		for i := range nInstances {
			curAlerts[i] = -1
			alerts, err := ams[i].GetAlerts()
			if err != nil {
				log.Warnf("error getting alerts for instance %d: %v", i, err)
				continue
			}
			curAlerts[i] = len(alerts)
		}

		log.Infof("alerts per AM: %v, %d notifications so far", curAlerts, wc.GetNNotifications())
	}
}

func prepareConfig(dir string) error {
	// Read the config file from embedded filesystem
	cfgData, err := testdataFS.ReadFile("testdata/alertmanager.test.yaml")
	if err != nil {
		return fmt.Errorf("failed to read embedded config file: %w", err)
	}

	if err := os.WriteFile(filepath.Join(dir, "alertmanager.env.yaml"), cfgData, 0600); err != nil {
		return fmt.Errorf("failed to write config file: %w", err)
	}

	return nil
}

func constructArgs(tempDir string, nInstances, id int) (string, []string) {
	args := append(baseAMArgs, []string{
		"--config.file=" + tempDir + "/alertmanager.env.yaml",
		fmt.Sprintf("--cluster.listen-address=[127.0.0.1]:%d", 9094+id),
		fmt.Sprintf("--web.listen-address=[127.0.0.1]:%d", 19093+id),
		fmt.Sprintf("--storage.path=%s/alertmanager_%d", tempDir, id),
	}...)
	for i := range nInstances {
		args = append(args, fmt.Sprintf("--cluster.peer=127.0.0.1:%d", 9094+i))
	}

	return fmt.Sprintf("http://127.0.0.1:%d/", 19093+id), args
}

func TestMain(m *testing.M) {
	var err error
	log, err = logging.New(
		logging.LevelInfo,
		logging.EncodingLogfmt,
	)
	if err != nil {
		fmt.Printf("error initializing logger: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}
