package main

import (
	"bytes"
	"fmt"
	"os/exec"
	"strings"
	"syscall"

	"github.com/fako1024/httpc"
	"github.com/prometheus/alertmanager/types"
)

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
