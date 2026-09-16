package mcptools

import (
	"fmt"
	"sort"
	"strings"
)

// inboxHeader is what a model can ask for when it answers by SendMessage
// rather than hub_send. SendMessage carries text and nothing else, so
// anything structural has to be said IN the text — which is why this is a
// parser between prose and the wire, and why it is built to fail loudly.
//
// Recognised only as the FIRST line, and only with the "#hub " prefix. A
// message that merely mentions to= or replyTo= in its body is prose and
// stays prose: the alternative is text meaning something structural
// wherever it appears, which is the class of bug this project has spent
// two days on.
//
// Deliberately NOT here: mentions and attachments. Mentions need a
// structure prose cannot carry unambiguously, and an attachment is a
// filesystem PATH in a message — a typo or a hallucinated filename would
// turn a sentence into a file read. Both keep hub_send, which takes them
// as real arguments rather than as text that looks like arguments.
type inboxHeader struct {
	To         string
	ReplyTo    string
	Confirm    string
	Format     string
	Present    bool
	Directives []string
}

// inboxHeaderPrefix marks a first line as directives rather than prose.
const inboxHeaderPrefix = "#hub "

// parseInboxHeader splits a relayed message into its directives and its
// body. An unrecognised key is an ERROR rather than prose: a typo'd
// "too=" silently relayed as text is the failure this exists to avoid,
// and a refusal the model can read beats a message that went somewhere
// else than it asked.
func parseInboxHeader(text string) (inboxHeader, string, error) {
	var h inboxHeader
	first, rest, _ := strings.Cut(text, "\n")
	if !strings.HasPrefix(first, inboxHeaderPrefix) {
		return h, text, nil
	}
	h.Present = true
	for _, field := range strings.Fields(strings.TrimPrefix(first, inboxHeaderPrefix)) {
		key, value, ok := strings.Cut(field, "=")
		if !ok || value == "" {
			return h, "", fmt.Errorf("%q is not key=value", field)
		}
		switch key {
		case "to":
			h.To = value
		case "replyTo":
			h.ReplyTo = value
		case "confirm":
			h.Confirm = value
		case "format":
			if value != "text" && value != "html" {
				return h, "", fmt.Errorf("format=%q — only text or html", value)
			}
			h.Format = value
		default:
			return h, "", fmt.Errorf("unknown directive %q (known: to, replyTo, confirm, format; "+
				"mentions and attachments need hub_send)", key)
		}
		h.Directives = append(h.Directives, key+"="+value)
	}
	if len(h.Directives) == 0 {
		return h, "", fmt.Errorf("a #hub line with no directives")
	}
	sort.Strings(h.Directives)
	return h, strings.TrimLeft(rest, "\n"), nil
}

// Summary renders what was parsed, for echoing back. A misparse should be
// visible in the result rather than inferred later from where the message
// ended up.
func (h inboxHeader) Summary() string {
	if !h.Present {
		return ""
	}
	return strings.Join(h.Directives, " ")
}
