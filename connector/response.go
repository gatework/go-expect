package connector

import (
	"context"
	"errors"
	"regexp"
	"strings"

	expect "github.com/gatework/go-expect"
)

// read consumes using the existing Expect matcher. Each segment shares the
// parent deadline and cumulative budget; page/confirmation traffic never resets
// either. Command error detection runs on the complete assembled response.
func (d *Device) read(ctx context.Context, s *expect.Session, c Command, prompt string, rules []Interaction, login bool, budget int) (receipt Receipt, err error) {
	var raw, output strings.Builder
	defer func() { receipt.Raw = raw.String(); receipt.Output = cleanOutput(output.String(), c.Text) }()
	cases := make([]expect.Case, 0, len(rules)+1)
	// Credential failures take priority on equal positions during login only.
	failures := 0
	if login {
		for _, expr := range d.profile.AuthenticationErrors {
			p, _ := pattern(expr)
			cases = append(cases, expect.Case{Pattern: p})
			failures++
		}
	}
	for _, rule := range rules {
		p, _ := pattern(rule.Pattern)
		cases = append(cases, expect.Case{Pattern: p})
	}
	p, _ := pattern(prompt)
	cases = append(cases, expect.Case{Pattern: p})
	counts := make([]int, len(rules))
	lastPager := ""
	appendRaw := func(text string) bool {
		n := min(len(text), budget-raw.Len())
		raw.WriteString(text[:n])
		return n == len(text)
	}
	for {
		result, e := s.Expect(ctx, cases, expect.WithTimeout(0))
		fitsBefore := appendRaw(result.Before)
		fitsMatch := appendRaw(result.Match)
		// Output is also bounded when the final fragment exceeds the budget.
		before := result.Before
		if lastPager != "" {
			before = stripPagerErase(before, lastPager)
			lastPager = ""
		}
		output.WriteString(before[:min(len(before), budget-output.Len())])
		if !fitsBefore || !fitsMatch {
			return receipt, errors.Join(ErrOutputLimit, e)
		}
		if e != nil {
			if errors.Is(e, expect.ErrBufferLimit) {
				e = errors.Join(ErrOutputLimit, e)
			}
			return receipt, e
		}
		if result.CaseIndex < failures {
			return receipt, ErrAuthentication
		}
		index := result.CaseIndex - failures
		if index == len(rules) {
			receipt.Prompt = strings.TrimRight(strings.TrimLeft(result.Match, "\r\n"), " \t")
			receipt.Complete = true
			prefix := len(result.Match) - len(strings.TrimLeft(result.Match, "\r\n"))
			output.WriteString(result.Match[:prefix])
			if err = ctx.Err(); err != nil {
				return receipt, err
			}
			if !login {
				for _, expr := range d.profile.CommandErrors {
					if regexp.MustCompile(expr).MatchString(cleanOutput(output.String(), c.Text)) {
						return receipt, ErrDevice
					}
				}
			}
			return receipt, nil
		}
		rule := rules[index]
		if !login && index >= len(rules)-len(d.profile.Paging) && !rule.Capture {
			lastPager = result.Match
		}
		if rule.Capture {
			output.WriteString(result.Match)
		}
		limit := rule.Limit
		if limit == 0 {
			limit = 1
		}
		counts[index]++
		if counts[index] > limit {
			return receipt, ErrResponseLimit
		}
		reply := rule.Reply
		if rule.Respond != nil {
			reply, err = rule.Respond(ctx, result)
			if err != nil {
				return receipt, err
			}
		}
		if err = ctx.Err(); err != nil {
			return receipt, err
		}
		if secret := strings.TrimRight(reply, "\r\n"); rule.Sensitive && secret != "" {
			if err = s.Redact(secret); err != nil {
				return receipt, err
			}
		}
		if _, err = s.Send(ctx, reply); err != nil {
			return receipt, err
		}
	}
}

// Remove only known immediate pager erasure sequences. Arbitrary terminal
// controls remain visible in Output and are rejected by built-in config
// validators; Raw always retains every received byte.
func stripPagerErase(text, marker string) string {
	width := len(marker)
	if width > 0 {
		back := strings.Repeat("\b", width)
		if strings.HasPrefix(text, back+strings.Repeat(" ", width)+back) {
			return text[len(back)*3:]
		}
		if strings.HasPrefix(text, back) {
			return text[len(back):]
		}
	}
	for _, sequence := range []string{"\r\x1b[2K", "\r\x1b[K", "\x1b[2K", "\x1b[K"} {
		if strings.HasPrefix(text, sequence) {
			return text[len(sequence):]
		}
	}
	return text
}
