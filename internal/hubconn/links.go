package hubconn

import (
	"encoding/json"
	"strings"
	"sync"
	"unicode"
)

// A message mirrored from a platform can carry externalLink, its address
// in that platform (Teams' own webUrl). It is never put in front of the
// model with the message — most messages are read without anyone wanting
// to open them — but remembered by externalId, for the reader to ask for
// one when it does.

// maxRememberedLinks bounds what one connection remembers; the oldest is
// forgotten first. A message older than that is read again with hub_read,
// which remembers it anew.
const maxRememberedLinks = 5000

// maxExternalLinkLen is the longest link accepted. Longer ones are
// dropped, as is anything that is not https.
const maxExternalLinkLen = 2048

type linkStore struct {
	mu    sync.Mutex
	byID  map[string]string
	order []string
}

// record remembers what externalID's message said about its link — ""
// for none, which is an answer too.
func (s *linkStore) record(externalID, link string) {
	if externalID == "" {
		return
	}
	if !validExternalLink(link) {
		link = ""
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.byID == nil {
		s.byID = map[string]string{}
	}
	if _, seen := s.byID[externalID]; !seen {
		s.order = append(s.order, externalID)
		if len(s.order) > maxRememberedLinks {
			delete(s.byID, s.order[0])
			s.order = s.order[1:]
		}
	}
	s.byID[externalID] = link
}

func (s *linkStore) lookup(externalID string) (link string, known bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	link, known = s.byID[externalID]
	return link, known
}

// validExternalLink accepts an https URL of printable characters only, so
// a link can be handed over verbatim without carrying anything else.
func validExternalLink(link string) bool {
	if len(link) > maxExternalLinkLen || !strings.HasPrefix(link, "https://") || len(link) == len("https://") {
		return false
	}
	for _, r := range link {
		if r > unicode.MaxASCII || !unicode.IsPrint(r) || r == ' ' {
			return false
		}
	}
	return true
}

// OffersExternalLinks reports whether the server declared that its
// messages carry links, so "this message has none" can be told apart
// from "this server never gives any".
func (c *Conn) OffersExternalLinks() bool {
	raw, ok := c.features["externalLinks"]
	if !ok {
		return false
	}
	var on bool
	return json.Unmarshal(raw, &on) == nil && on
}

// ExternalLink is the link of the message with externalID among those
// this connection received recently, and whether that message is among
// them at all.
func (c *Conn) ExternalLink(externalID string) (link string, known bool) {
	return c.links.lookup(externalID)
}
