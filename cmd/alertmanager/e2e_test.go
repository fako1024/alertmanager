package main

import (
	"bytes"
	"embed"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/els0r/telemetry/logging"
	"github.com/fako1024/httpc"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/common/model"
)

//go:embed testdata/alertmanager.test.yaml
var testdataFS embed.FS

const (
	nInstances         = 10
	nAlerts            = 10000
	statusInfoInterval = 5 * time.Second

	binary = "./alertmanager"
	// alertsEndpoint = "http://127.0.0.1:19093/api/v2/alerts"
)

var (
	log            *logging.L
	retryIntervals = httpc.Intervals{100 * time.Millisecond, 500 * time.Millisecond, time.Second}
)

func TestXxx(t *testing.T) {

	log.Infof("Starting %d Alertmanager instances, waiting for gossip to settle...", nInstances)

	tempDir := t.TempDir()
	if err := prepareConfig(tempDir); err != nil {
		t.Fatalf("Failed to prepare config: %v", err)
	}

	webhookConsumer := newWebhookConsumer()
	go webhookConsumer.ListenAndServe(":10025")

	ams := make(AMInstances, nInstances)
	for i := range nInstances {
		ams[i] = NewAMInstance(getArgs(tempDir, nInstances, i))
	}

	err := ams.Start()
	if err != nil {
		t.Fatalf("Failed to start Alertmanager instances: %v", err)
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
				log.Errorf("Error getting alerts: %v", err)
			}
			log.Infof("Received %d alerts, %d notifications so far (healthy: %v, ready: %v)", len(alerts), webhookConsumer.GetNNotifications(), healthy, ready)
		}
	}()

	log.Infof("All instances ready / healthy - Sending %d alerts...", nAlerts)

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
				},
			}); err != nil {
				t.Fatalf("Error sending alert: %v", err)
			}
		}
	}

	log.Infof("Sent %d alerts, waiting for state to settle...", nAlerts)
	time.Sleep(300 * time.Second)

	log.Info("Restarting Alertmanager instances...")

	for range 100 {
		wg := &sync.WaitGroup{}
		for k := range 3 {
			wg.Add(1)
			go func(j int) {
				if err := ams[j].Restart(); err != nil {
					log.Errorf("Error restarting Alertmanager instance: %v", err)
				}
				wg.Done()
			}(k)
		}
		wg.Wait()

		time.Sleep(1 * time.Second)
	}

	err = ams.Stop()
	if err != nil {
		t.Fatalf("Failed to stop Alertmanager instances: %v", err)
	}
}

type AMInstances []*AMInstance

func (a AMInstances) Start() error {
	for _, instance := range a {
		if err := instance.Start(); err != nil {
			return fmt.Errorf("failed to start instance: %w", err)
		}
	}
	return nil
}

func (a AMInstances) Stop() error {
	for _, instance := range a {
		if err := instance.Stop(); err != nil {
			return fmt.Errorf("failed to stop instance: %w", err)
		}
	}
	return nil
}

func (a AMInstances) Health() error {
	for _, instance := range a {
		if err := instance.Health(); err != nil {
			return fmt.Errorf("failed to get health: %w", err)
		}
	}
	return nil
}

func (a AMInstances) GossipSettled() bool {
	for _, instance := range a {
		if !instance.GossipSettled() {
			return false
		}
	}
	return true
}

func (a AMInstances) Ready() error {
	for _, instance := range a {
		if err := instance.Ready(); err != nil {
			return fmt.Errorf("failed to get ready state: %w", err)
		}
	}
	return nil
}

type AMInstance struct {
	Endpoint string
	Args     []string

	cmd *exec.Cmd
	out *bytes.Buffer
}

func NewAMInstance(endpoint string, args []string) *AMInstance {
	return &AMInstance{
		Endpoint: endpoint,
		Args:     args,
		out:      bytes.NewBuffer(nil),
	}
}

func (a *AMInstance) Start() error {
	a.cmd = exec.Command(binary, a.Args...)
	a.cmd.Stdout = a.out
	a.cmd.Stderr = a.out

	if err := a.cmd.Start(); err != nil {
		return fmt.Errorf("failed to start command: %w\n%v", err, a.out.String())
	}

	return nil
}

func (a *AMInstance) Stop() error {
	if err := a.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("failed to send TERM signal: %w", err)
	}

	if err := a.cmd.Wait(); err != nil {
		if err.Error() == "signal: terminated" {
			return nil
		}
		return err
	}

	return nil
}

func (a *AMInstance) Restart() error {
	if err := a.Stop(); err != nil {
		return err
	}
	return a.Start()
}

func (a *AMInstance) Logs() string {
	return a.out.String()
}

func (a *AMInstance) Health() error {
	if err := httpc.New("GET", a.Endpoint+"-/healthy").RetryBackOff(retryIntervals).Run(); err != nil {
		return fmt.Errorf("failed to get health: %v", err)
	}
	return nil
}

func (a *AMInstance) Ready() error {
	if err := httpc.New("GET", a.Endpoint+"-/ready").RetryBackOff(retryIntervals).Run(); err != nil {
		return fmt.Errorf("failed to get health: %v", err)
	}
	return nil
}

func (a *AMInstance) GossipSettled() bool {
	return strings.Contains(a.out.String(), "gossip settled; proceeding")
}

func (a *AMInstance) SendAlert(alert types.Alert) error {
	return httpc.New("POST", a.Endpoint+"api/v2/alerts").RetryBackOff(retryIntervals).EncodeJSON([]*types.Alert{
		&alert,
	}).Run()
}

func (a *AMInstance) GetAlerts() ([]types.Alert, error) {
	var alerts []types.Alert
	if err := httpc.New("GET", a.Endpoint+"api/v2/alerts").RetryBackOff(retryIntervals).ParseJSON(&alerts).Run(); err != nil {
		return nil, fmt.Errorf("failed to get alerts: %v", err)
	}
	return alerts, nil
}

// prepareConfig reads the embedded config file and writes it to the specified directory
func prepareConfig(dir string) error {
	// Read the config file from embedded filesystem
	cfgData, err := testdataFS.ReadFile("testdata/alertmanager.test.yaml")
	if err != nil {
		return fmt.Errorf("failed to read embedded config file: %v", err)
	}

	if err := os.WriteFile(dir+"/alertmanager.env.yaml", cfgData, 0644); err != nil {
		return fmt.Errorf("failed to write config file: %v", err)
	}

	return nil
}

// Call:
// /bin/alertmanager --config.file=/etc/alertmanager/config_out/alertmanager.env.yaml --storage.path=/alertmanager --data.retention=2160h --cluster.listen-address=[10.32.16.96]:9094 --web.listen-address=:9093 --web.external-url=https://alertmgr.osdp.open.ch --web.route-prefix=/ --cluster.pushpull-interval=5s --cluster.peer=alertmanager-osdp-monitoring-alertmanager-0.alertmanager-operated:9094 --cluster.peer=alertmanager-osdp-monitoring-alertmanager-1.alertmanager-operated:9094 --cluster.peer=alertmanager-osdp-monitoring-alertmanager-2.alertmanager-operated:9094 --cluster.reconnect-timeout=5m --web.config.file=/etc/alertmanager/web_config/web-config.yaml
func getArgs(tempDir string, nInstances, id int) (string, []string) {
	args := []string{
		"--data.retention=1h",
		"--web.route-prefix=/",
		"--cluster.pushpull-interval=5s",
		"--cluster.reconnect-timeout=15s",
		"--config.file=" + tempDir + "/alertmanager.env.yaml",
		fmt.Sprintf("--cluster.listen-address=[127.0.0.1]:%d", 9094+id),
		fmt.Sprintf("--web.listen-address=[127.0.0.1]:%d", 19093+id),
		fmt.Sprintf("--storage.path=%s/alertmanager_%d", tempDir, id),
	}
	for i := 0; i < nInstances; i++ {
		args = append(args, fmt.Sprintf("--cluster.peer=127.0.0.1:%d", 9094+i))
	}

	return fmt.Sprintf("http://127.0.0.1:%d/", 19093+id), args
}

type webhookConsumer struct {
	nReceived uint64
}

func newWebhookConsumer() *webhookConsumer {
	return &webhookConsumer{
		nReceived: 0,
	}
}

func (wc *webhookConsumer) ListenAndServe(endpoint string) error {
	// Start a simple HTTP server to handle incoming webhook requests
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		var alerts ReceivedAlerts
		if json.NewDecoder(r.Body).Decode(&alerts) != nil {
			http.Error(w, "Failed to decode JSON", http.StatusBadRequest)
			return
		}
		atomic.AddUint64(&wc.nReceived, uint64(len(alerts.Alerts)))
		w.WriteHeader(http.StatusOK)
	})

	return http.ListenAndServe(endpoint, nil)
}

func (wc *webhookConsumer) GetNNotifications() uint64 {
	return atomic.LoadUint64(&wc.nReceived)
}

type ReceivedAlerts struct {
	Receiver        string        `json:"receiver"`
	Status          string        `json:"status"`
	Alerts          []types.Alert `json:"alerts"`
	ExternalURL     string        `json:"externalURL"`
	Version         string        `json:"version"`
	GroupKey        string        `json:"groupKey"`
	TruncatedAlerts int           `json:"truncatedAlerts"`
}

func TestMain(m *testing.M) {
	var err error
	log, err = logging.New(
		logging.LevelInfo,
		logging.EncodingLogfmt,
	)
	if err != nil {
		fmt.Printf("Error initializing logger: %v\n", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}
