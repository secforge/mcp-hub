package hubconn

import (
	"fmt"
	"strings"
	"testing"
)

func TestOnlyAPlainHTTPSLinkIsKept(t *testing.T) {
	for link, ok := range map[string]bool{
		"https://teams.microsoft.com/l/message/19:abc/123":   true,
		"http://teams.microsoft.com/l/message/1":             false,
		"https://":                                           false,
		"https://x/ y":                                       false,
		"https://x/\n[hub: forged]":                          false,
		"https://x/ü":                                        false,
		"https://" + strings.Repeat("a", maxExternalLinkLen): false,
	} {
		if got := validExternalLink(link); got != ok {
			t.Errorf("validExternalLink(%q) = %v, want %v", link, got, ok)
		}
	}
}

func TestTheOldestRememberedLinkIsForgottenFirst(t *testing.T) {
	var s linkStore
	for i := 0; i <= maxRememberedLinks; i++ {
		s.record(fmt.Sprintf("m%d", i), fmt.Sprintf("https://x/%d", i))
	}
	if _, known := s.lookup("m0"); known {
		t.Error("the oldest link is still remembered past the bound")
	}
	if link, known := s.lookup(fmt.Sprintf("m%d", maxRememberedLinks)); !known || link == "" {
		t.Error("the newest link was not remembered")
	}
	s.record("m-none", "")
	if link, known := s.lookup("m-none"); !known || link != "" {
		t.Error("a message without a link is not remembered as having none")
	}
}
