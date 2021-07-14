package cockroachdb

import (
	_ "embed"
	"fmt"

	"github.com/shanduur/squat/config"
	"github.com/shanduur/squat/providers"
)

type cockroachCfg struct {
	ProviderName string            `toml:"provider-name"`
	Formats      providers.Formats `toml:"formats"`
}

//go:embed describe.sql
var describeQuery string

type Provider struct {
	cfg cockroachCfg
}

func New(configPath string) (Provider, error) {
	var p Provider

	err := p.Initialize(configPath)
	if err != nil {
		return p, fmt.Errorf("unable to initialize: %s", err.Error())
	}

	return p, nil
}

func (p *Provider) Initialize(configPath string) (err error) {
	err = config.ReadTOML(&p.cfg, configPath)
	if err != nil {
		return fmt.Errorf("unable to read config: %s", err.Error())
	}

	return nil
}

func (p Provider) ProviderName() string {
	return p.cfg.ProviderName
}

func (p Provider) GetTableDescription(string) ([]providers.Describe, error) {
	return []providers.Describe{}, nil
}

func (p Provider) DateFormat() string {
	return p.cfg.Formats.DateFormat
}

func (p Provider) DateTimeFormat() string {
	return p.cfg.Formats.DateTimeFormat
}
