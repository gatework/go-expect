package expect

import (
	"context"
	"errors"
)

// Response sends a reply or performs a caller-defined action for a matched rule.
// It must return write errors; actions are never automatically retried.
type Response func(context.Context, *Session, Result) error

// Rule describes a reusable automatic response. Limit zero means one response
// per Expect, negative is invalid, positive sets an explicit finite limit.
type Rule struct {
	Pattern Pattern
	Respond Response
	Limit   int
}

func (Rule) String() string     { return "expect.Rule" }
func (r Rule) GoString() string { return r.String() }

// Reply returns an action that sends text verbatim, including explicit line ends.
func Reply(text string) Response {
	return func(ctx context.Context, s *Session, _ Result) error { _, err := s.Send(ctx, text); return err }
}

// Responder is an immutable rule set, safe to reuse across different sessions.
// Rule counts are local to each call; callback closures remain caller-owned.
type Responder struct{ rules []Rule }

func (r *Responder) String() string   { return "expect.Responder" }
func (r *Responder) GoString() string { return r.String() }

// NewResponder validates and snapshots rules without accessing a session.
func NewResponder(rules ...Rule) (*Responder, error) {
	copyRules := append([]Rule(nil), rules...)
	for i := range copyRules {
		rule := &copyRules[i]
		if err := rule.Pattern.validate(true); err != nil {
			return nil, err
		}
		if rule.Respond == nil || rule.Limit < 0 {
			return nil, errors.New("expect: invalid response rule")
		}
		if rule.Limit == 0 {
			rule.Limit = 1
		}
	}
	return &Responder{rules: copyRules}, nil
}

// Expect responds to registered prompts while waiting for a completion pattern.
// Completion wins only same-position ties. Responses preserve the original
// relative timer. The returned CaseIndex indexes completion, or is -1 on failure.
func (r *Responder) Expect(ctx context.Context, s *Session, completion []Pattern, opts ...ExpectOption) (Result, error) {
	if r == nil || s == nil || len(completion) == 0 {
		err := errors.New("expect: responder requires a session and completion")
		if s != nil {
			s.notify(Event{Operation: "expect", CaseIndex: -1, Error: errorClass(err)})
		}
		return Result{CaseIndex: -1}, err
	}
	cases := make([]Case, 0, len(completion)+len(r.rules))
	guards := make([]func(Result) error, 0, cap(cases))
	for _, pattern := range completion {
		cases = append(cases, Case{Pattern: pattern})
		guards = append(guards, nil)
	}
	for i, rule := range r.rules {
		count := 0
		guards = append(guards, func(Result) error {
			if count >= rule.Limit {
				return &ResponseError{RuleIndex: i}
			}
			return nil
		})
		cases = append(cases, Case{Pattern: rule.Pattern, Handler: func(ctx context.Context, s *Session, result Result) (Action, error) {
			count++
			return ContinueWithoutReset, rule.Respond(ctx, s, result)
		}})
	}
	result, err := s.expect(ctx, cases, guards, opts...)
	if result.CaseIndex >= len(completion) {
		result.CaseIndex = -1
	}
	s.notifyExpect(result, err)
	return result, err
}
