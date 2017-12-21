package kubernetes

import (
	"bytes"
	"fmt"
	"io"
	"os/exec"
	"strings"
	"time"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"
	"github.com/weaveworks/flux/cluster"
	rest "k8s.io/client-go/rest"
)

var (
	cmds = []string{"delete", "apply"}
)

type Kubectl struct {
	exe    string
	config *rest.Config

	changeSet
}

func NewKubectl(exe string, config *rest.Config) *Kubectl {
	return &Kubectl{
		exe:    exe,
		config: config,
	}
}

func (c *Kubectl) connectArgs() []string {
	var args []string
	if c.config.Host != "" {
		args = append(args, fmt.Sprintf("--server=%s", c.config.Host))
	}
	if c.config.Username != "" {
		args = append(args, fmt.Sprintf("--username=%s", c.config.Username))
	}
	if c.config.Password != "" {
		args = append(args, fmt.Sprintf("--password=%s", c.config.Password))
	}
	if c.config.TLSClientConfig.CertFile != "" {
		args = append(args, fmt.Sprintf("--client-certificate=%s", c.config.TLSClientConfig.CertFile))
	}
	if c.config.TLSClientConfig.CAFile != "" {
		args = append(args, fmt.Sprintf("--certificate-authority=%s", c.config.TLSClientConfig.CAFile))
	}
	if c.config.TLSClientConfig.KeyFile != "" {
		args = append(args, fmt.Sprintf("--client-key=%s", c.config.TLSClientConfig.KeyFile))
	}
	if c.config.BearerToken != "" {
		args = append(args, fmt.Sprintf("--token=%s", c.config.BearerToken))
	}
	return args
}

func (c *Kubectl) execute(logger log.Logger, errs cluster.SyncError) {
	defer c.changeSet.clear()

	for _, cmd := range cmds {
		defaultSet := newExecuteSet(cmd, "--namespace", "default")
		otherSet := newExecuteSet(cmd)

		for _, obj := range c.objs[cmd] {
			set := defaultSet
			if !obj.hasDefaultNamespace() {
				set = otherSet
			}
			set.stage(obj)
		}

		c.exec(logger, defaultSet, cmd, errs)
		c.exec(logger, otherSet, cmd, errs)
	}
}

func (c *Kubectl) exec(logger log.Logger, s *executeSet, cmd string, errs cluster.SyncError) {
	if len(s.objs) == 0 {
		return
	}
	// Attempt to apply everything in s at once, else fallback to applying one at a time.
	if err := c.doCommand(logger, s.rw, s.cmd...); err != nil {
		for _, obj := range s.objs {
			r := bytes.NewReader(obj.bytes)
			if err := c.doCommand(logger, r, s.cmd...); err != nil {
				errs[obj.id] = err
			}
		}
	}
}

type executeSet struct {
	cmd  []string
	rw   io.ReadWriter
	objs []obj
}

func newExecuteSet(cmd ...string) *executeSet {
	return &executeSet{
		cmd: cmd,
		rw:  &bytes.Buffer{},
	}
}

func (s *executeSet) stage(o obj) {
	s.objs = append(s.objs, o)
	fmt.Fprintln(s.rw, "---\n"+string(o.bytes))
}

func (c *Kubectl) doCommand(logger log.Logger, r io.Reader, args ...string) error {
	args = append(args, "-f", "-")
	cmd := c.kubectlCommand(args...)
	cmd.Stdin = r
	stderr := &bytes.Buffer{}
	cmd.Stderr = stderr
	stdout := &bytes.Buffer{}
	cmd.Stdout = stdout

	begin := time.Now()
	err := cmd.Run()
	if err != nil {
		err = errors.Wrap(errors.New(strings.TrimSpace(stderr.String())), "running kubectl")
	}

	logger.Log("cmd", "kubectl "+strings.Join(args, " "), "took", time.Since(begin), "err", err, "output", strings.TrimSpace(stdout.String()))
	return err
}

func (c *Kubectl) kubectlCommand(args ...string) *exec.Cmd {
	return exec.Command(c.exe, append(c.connectArgs(), args...)...)
}

type changeSet struct {
	objs map[string][]obj
}

func (c *changeSet) stage(cmd, id string, o *apiObject) {
	if c.objs == nil {
		c.objs = make(map[string][]obj)
	}
	c.objs[cmd] = append(c.objs[cmd], obj{id, o})
}

func (c *changeSet) clear() {
	if c.objs == nil {
		c.objs = make(map[string][]obj)
		return
	}
	for cmd := range c.objs {
		c.objs[cmd] = nil
	}
}

type obj struct {
	id string
	*apiObject
}
