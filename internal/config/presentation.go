package config

import (
	"errors"
	"net/url"
	"strings"
	"unicode"
)

// Presentation contains optional deployment text, never protocol identifiers.
type Presentation struct {
	Organization     string `yaml:"organization" json:"organization"`
	DisplayName      string `yaml:"display_name" json:"display_name"`
	DocumentationURL string `yaml:"documentation_url" json:"documentation_url"`
	SupportURL       string `yaml:"support_url" json:"support_url"`
	MOTD             string `yaml:"motd" json:"motd"`
}

// Validate rejects terminal control sequences and unsafe links before display.
func (presentation Presentation) Validate() error {
	for _, text := range []string{presentation.Organization, presentation.DisplayName} {
		if !safePresentationText(text, 256, false) {
			return errors.New("presentation labels must contain at most 256 bytes of plain single-line text")
		}
	}
	if !safePresentationText(presentation.MOTD, 4096, true) {
		return errors.New("presentation motd must contain at most 4096 bytes of plain text")
	}
	for _, link := range []string{presentation.DocumentationURL, presentation.SupportURL} {
		if link == "" {
			continue
		}
		parsed, err := url.Parse(link)
		if err != nil || len(link) > 2048 || !safePresentationText(link, 2048, false) ||
			parsed.Scheme != "https" || parsed.Host == "" || parsed.User != nil {
			return errors.New("presentation links must be HTTPS URLs without credentials")
		}
	}
	return nil
}

func safePresentationText(text string, limit int, multiline bool) bool {
	if len(text) > limit {
		return false
	}
	return !strings.ContainsFunc(text, func(character rune) bool {
		return (unicode.IsControl(character) && (!multiline || character != '\n')) || unicode.Is(unicode.Cf, character)
	})
}
