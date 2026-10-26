package connector

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"
)

func TestConfigurationPreflight(t *testing.T) {
	tests := []struct {
		name string
		cfg  Config
	}{
		{"empty host", Config{}}, {"option host", Config{Host: "-proxy"}},
		{"credential injection", Config{Host: "example", Password: "a\nb"}},
		{"protocol", Config{Host: "example", Protocol: "auto"}},
		{"port", Config{Host: "example", Port: 65536}},
		{"timeout", Config{Host: "example", LoginTimeout: -time.Second}},
		{"challenge", Config{Host: "example", Challenges: []Interaction{{Pattern: ".*"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := New(CiscoIOS(), tt.cfg); !errors.Is(err, ErrInvalid) {
				t.Fatalf("accepted invalid config: %v", err)
			}
		})
	}
	for _, p := range []Profile{CiscoIOS(), Huawei(), H3C()} {
		if _, err := New(p, Config{Host: "example"}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestParseScriptPreflight(t *testing.T) {
	commands, err := ParseScript("# comment\r\n\r\n show version\r\ndescription a#b\n")
	if err != nil || len(commands) != 2 || commands[0].Line != 3 || commands[0].Text != " show version" || commands[1].Text != "description a#b" {
		t.Fatalf("script parse: %v %#v", err, commands)
	}
	for _, text := range []string{"show\nother\x00", "show\nother\rhidden"} {
		if _, err := ParseScript(text); !errors.Is(err, ErrInvalid) {
			t.Fatal("invalid later command accepted")
		}
	}
}

func TestSafeFormatting(t *testing.T) {
	values := []any{
		Config{Password: "sensitive-secret"}, Command{Text: "sensitive-secret"},
		Receipt{Command: "sensitive-secret", Raw: "sensitive-secret", Output: "sensitive-secret", Err: failure("test", errors.New("sensitive-secret"))},
		Configuration{Text: "sensitive-secret"}, Result{Steps: []Receipt{{Output: "sensitive-secret"}}},
	}
	for _, v := range values {
		if strings.Contains(fmt.Sprintf("%v %+v %#v", v, v, v), "sensitive-secret") {
			t.Fatal("format leaked payload")
		}
		data, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(string(data), "sensitive-secret") {
			t.Fatal("JSON leaked payload")
		}
	}
}
