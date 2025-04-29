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

	go func() {
		for {
			time.Sleep(statusInfoInterval)
			healthy, ready := ams.Health() == nil, ams.Ready() == nil
			alerts, err := ams[0].GetAlerts()
			if err != nil {
				log.Warnf("error getting alerts: %v", err)
			}
			log.Infof("received %d alerts, %d notifications so far (healthy: %v, ready: %v)", len(alerts), webhookConsumer.GetNNotifications(), healthy, ready)
		}
	}()

	log.Infof("all instances ready / healthy - Sending %d alerts...", nAlerts)

	for i := range nAlerts {
		// TODO: Try round-robin instead of sent-to-all (maybe fixes it)?
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

	log.Infof("sent %d alerts, waiting for state to settle...", nAlerts)
	for {
		time.Sleep(1 * time.Second)
		alerts, err := ams[0].GetAlerts()
		if err != nil {
			log.Errorf("error getting alerts: %v", err)
		}
		nNotifications := webhookConsumer.GetNNotifications()
		if uint64(len(alerts)) == nNotifications {
			log.Infof("alerts / notifications equalized (%d/%d), continuing...", len(alerts), nNotifications)
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
