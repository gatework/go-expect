package connector

import (
	"regexp"
	"strings"
)

// Profile supplies explicit device policy. New snapshots all slices. Function
// closures are borrowed and must be safe for their intended reuse.
type Profile struct {
	Name                                        string
	LoginPrompt, UsernamePrompt, PasswordPrompt string
	AuthenticationErrors, CommandErrors         []string
	Paging                                      []Interaction
	Enable                                      Command
	RunningConfig                               Command
	// ValidateConfig must reject incomplete output and return a cleaned copy.
	ValidateConfig func(string) (string, error)
	SaveConfig     Command
	// SaveSuccess is required for SaveConfig; prompt alone is not save evidence.
	SaveSuccess string
}

func (p Profile) clone() (Profile, error) {
	if _, err := pattern(p.LoginPrompt); err != nil {
		return p, err
	}
	if p.UsernamePrompt == "" {
		p.UsernamePrompt = `(?i)(?:login|username):[ \t]*\z`
	}
	if p.PasswordPrompt == "" {
		p.PasswordPrompt = `(?i)password:[ \t]*\z`
	}
	if len(p.AuthenticationErrors) == 0 {
		p.AuthenticationErrors = []string{`(?i)(?:authentication failed|login incorrect|permission denied|host key verification failed|REMOTE HOST IDENTIFICATION HAS CHANGED)`}
	}
	for _, expr := range append(append([]string{p.UsernamePrompt, p.PasswordPrompt}, p.AuthenticationErrors...), p.CommandErrors...) {
		if _, err := pattern(expr); err != nil {
			return p, err
		}
	}
	if err := validateInteractions(p.Paging); err != nil {
		return p, err
	}
	var err error
	for _, c := range []*Command{&p.Enable, &p.RunningConfig, &p.SaveConfig} {
		if c.Text != "" {
			*c, err = snapshot(*c)
			if err != nil {
				return p, err
			}
		}
	}
	if p.SaveSuccess != "" {
		if _, err = pattern(p.SaveSuccess); err != nil {
			return p, err
		}
	}
	p.Paging = append([]Interaction(nil), p.Paging...)
	p.AuthenticationErrors = append([]string(nil), p.AuthenticationErrors...)
	p.CommandErrors = append([]string(nil), p.CommandErrors...)
	return p, nil
}

// CiscoIOS describes common IOS/IOS XE dialogues. Validate on target firmware.
// Privilege escalation remains explicit through Device.Enable.
func CiscoIOS() Profile {
	return Profile{
		Name: "cisco_ios", LoginPrompt: `(?:\A|[\r\n])[a-zA-Z0-9_.:/()-]+[>#][ \t]*\z`,
		Paging:         []Interaction{{Pattern: `(?i)--More--[ \t]*`, Reply: " ", Limit: 1000}},
		CommandErrors:  []string{`(?im)^[ \t]*%[ \t]*(?:Invalid|Incomplete|Ambiguous|Error|Unknown)\b`},
		Enable:         Command{Text: "enable", Prompt: `(?:\A|[\r\n])[a-zA-Z0-9_.:/()-]+#[ \t]*\z`},
		RunningConfig:  Command{Text: "show running-config"},
		ValidateConfig: func(s string) (string, error) { return terminatedConfig(s, "end") },
		SaveConfig: Command{Text: "copy running-config startup-config", Interactions: []Interaction{
			{Pattern: `(?i)Destination filename[^\r\n]*\?[ \t]*\z`, Reply: "\r"},
			{Pattern: `(?i)Proceed with copy\?[ \t]*\[confirm\][ \t]*\z`, Reply: "\r"},
		}}, SaveSuccess: `(?m)\[OK\]`,
	}
}

// Huawei describes the basic VRP user-view collection and save dialogue.
func Huawei() Profile {
	return Profile{
		Name: "huawei", LoginPrompt: `(?:\A|[\r\n])(?:<[^<>\r\n]+>|\[[^\[\]\r\n]+\])[ \t]*\z`,
		Paging:         []Interaction{{Pattern: `(?i)----[ \t]*More[^\r\n]*----[ \t]*`, Reply: " ", Limit: 1000}},
		CommandErrors:  []string{`(?im)^[ \t]*(?:Error:|%[ \t]*(?:Unrecognized|Incomplete|Ambiguous))`},
		RunningConfig:  Command{Text: "display current-configuration"},
		ValidateConfig: func(s string) (string, error) { return terminatedConfig(s, "return") },
		SaveConfig:     Command{Text: "save force"},
		SaveSuccess:    `(?im)^[ \t]*(?:Info:[ \t]*)?(?:Save the configuration successfully\.|The configuration has been saved successfully\.|Configuration is saved to device successfully\.)[ \t]*$`,
	}
}

// H3C describes basic Comware collection; saving has command-scoped confirmation.
func H3C() Profile {
	p := Huawei()
	p.Name = "h3c"
	p.SaveConfig = Command{Text: "save safely", Interactions: []Interaction{
		{Pattern: `(?i)(?:continue|are you sure)[^\r\n]*\[Y/N\][^\r\n]*\z`, Reply: "y\r"},
		{Pattern: `(?i)(?:file name|filename)[^\r\n]*\]:?[ \t]*\z`, Reply: "\r"},
	}}
	return p
}

func terminatedConfig(s, terminator string) (string, error) {
	for _, r := range s {
		if (r < 32 && r != '\n' && r != '\t') || r == 127 {
			return "", ErrIncomplete
		}
	}
	trimmed := strings.TrimSpace(s)
	lines := strings.Split(trimmed, "\n")
	if len(lines) < 2 || strings.TrimSpace(lines[len(lines)-1]) != terminator {
		return "", ErrIncomplete
	}
	return trimmed + "\n", nil
}

func exactPrompt(prompt string) string {
	return `(?:\A|[\r\n])` + regexp.QuoteMeta(prompt) + `[ \t]*\z`
}
