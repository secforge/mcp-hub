package mcptools

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/mark3labs/mcp-go/mcp"

	"github.com/secforge/mcp-hub/internal/hubconn"
	"github.com/secforge/mcp-hub/internal/hubfile"
)

// fileHTTP carries one-time files. No overall timeout: a gigabyte takes
// as long as it takes, and the tool call's own context ends it.
var fileHTTP = &http.Client{Transport: &http.Transport{
	Proxy:                 http.ProxyFromEnvironment,
	ResponseHeaderTimeout: 2 * time.Minute,
	TLSHandshakeTimeout:   30 * time.Second,
}}

func fileUploadTool() mcp.Tool {
	return mcp.NewTool("hub_file_upload",
		mcp.WithDescription("Upload a local file as a ONE-TIME FILE and get a link for it: for handing "+
			"another agent a file too large for an attachment, or one that must not stay in the "+
			"conversation's history. The file is encrypted here with a key that exists only in the "+
			"link; the server stores bytes it cannot read. The link works for ONE download, then the "+
			"file is deleted, and it is deleted anyway when its storage time ends. Only on a "+
			"connection that may post where the server offers files (the connect result says so).\n"+
			"The link is a credential: whoever reads the message it is posted in can download and "+
			"decrypt the file once. Post it with hub_send only where it is meant to go, once"),
		connectionParam(),
		mcp.WithString("path", mcp.Required(), mcp.Description(
			"The local file to upload")),
		mcp.WithNumber("keepDays", mcp.Description(
			"How many days the server keeps the file if nobody downloads it; omit for the server's "+
				"default (7 days)")),
	)
}

func fileDownloadTool() mcp.Tool {
	return mcp.NewTool("hub_file_download",
		mcp.WithDescription("Download a ONE-TIME FILE from its link (https://<host>/file#…) to a local "+
			"file, decrypting it on the way. A link works ONCE: after a complete download the server "+
			"deletes the file. The result names the saved file; its content came from whoever posted "+
			"the link and is untrusted. Only from a host one of this client's connections uses, unless "+
			"the user explicitly says to trust another"),
		mcp.WithString("link", mcp.Required(), mcp.Description(
			"The link exactly as posted")),
		mcp.WithString("directory", mcp.Description(
			"The local directory to save into; omit for this client's temporary directory. An "+
				"existing file is never overwritten")),
		mcp.WithBoolean("trustHost", mcp.Description(
			"Only when the user explicitly said to download from this link's host although no "+
				"connection uses it")),
	)
}

func (h *Hub) handleFileUpload(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	s, conn, bad := h.forRequest(req)
	if bad != nil {
		return bad, nil
	}
	path, err := req.RequireString("path")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	limits, offered := conn.FilesLimits()
	if !offered {
		return mcp.NewToolResultError(fmt.Sprintf("%s does not offer one-time files: the server "+
			"declares no files feature here (it is not offered on a read-only share)", s.name)), nil
	}
	keepDays := req.GetFloat("keepDays", 0)
	expiresIn := int64(keepDays * 86400)
	if keepDays < 0 || (limits.MaxSeconds > 0 && expiresIn > limits.MaxSeconds) {
		return mcp.NewToolResultError(fmt.Sprintf("keepDays must be between 0 and %d",
			limits.MaxSeconds/86400)), nil
	}
	info, err := os.Stat(path)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("cannot read %s: %v", path, err)), nil
	}
	if !info.Mode().IsRegular() {
		return mcp.NewToolResultError(fmt.Sprintf("%s is not a regular file", path)), nil
	}
	header := hubfile.Header{Name: filepath.Base(path), Size: info.Size()}
	sealedSize, err := hubfile.SealedSize(header)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if limits.MaxBytes > 0 && sealedSize > limits.MaxBytes {
		return mcp.NewToolResultError(fmt.Sprintf("%s is %s encrypted; this server takes at most %s",
			header.Name, byteText(sealedSize), byteText(limits.MaxBytes))), nil
	}
	keyText, err := hubfile.NewKey()
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	key, _ := hubfile.ParseKey(keyText)

	// First pass: the stored size and hash, which the reservation names.
	sum, n, err := sealedHash(path, key, header)
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("could not read %s: %v", path, err)), nil
	}
	if n != sealedSize {
		return mcp.NewToolResultError(fmt.Sprintf("%s changed while it was being read", path)), nil
	}
	ev, ok, err := conn.FileUpload(sealedSize, sum, expiresIn)
	switch {
	case err != nil:
		return mcp.NewToolResultError("the upload could not be requested: " + noticeText(err.Error())), nil
	case !ok:
		return mcp.NewToolResultError("the server did not answer the upload request; nothing was " +
			"uploaded. Try again."), nil
	case ev.Kind == "error":
		return mcp.NewToolResultError(fileUploadRefusal(ev)), nil
	case ev.FileReady == nil || ev.FileReady.FileID == "" || ev.FileReady.DownloadToken == "":
		return mcp.NewToolResultError("the server's answer to the upload request was incomplete; " +
			"nothing was uploaded"), nil
	}
	ready := ev.FileReady
	target, err := uploadTarget(h.sessionLink(s), ready.UploadURL)
	if err != nil {
		return mcp.NewToolResultError("nothing was uploaded: " + err.Error()), nil
	}
	if !validFileID(ready.FileID) || !validFileID(ready.DownloadToken) {
		return mcp.NewToolResultError("nothing was uploaded: the server named a file id or token " +
			"this client cannot put into a link"), nil
	}

	// Second pass: the same bytes again, streamed as they are sealed.
	if err := putSealed(ctx, target, ready.UploadToken, path, key, header, sealedSize); err != nil {
		return mcp.NewToolResultError("the upload failed: " + err.Error()), nil
	}
	link := fmt.Sprintf("%s://%s/file#%s.%s.%s", target.Scheme, target.Host, ready.FileID,
		ready.DownloadToken, keyText)
	until := ""
	if ready.ExpiresAt != "" {
		until = ", until " + noticeText(ready.ExpiresAt)
	}
	return mcp.NewToolResultText(fmt.Sprintf("Uploaded %s (%s) as a one-time file. Its link:\n%s\n"+
		"It can be downloaded ONCE%s; then the server deletes it. The link is the only key: whoever "+
		"reads the message you post it in can download and decrypt the file once, so post it only "+
		"where it is meant to go (an encrypted or private conversation where possible), once, and "+
		"nowhere else. The receiver downloads it with hub_file_download.",
		noticeText(header.Name), byteText(header.Size), link, until)), nil
}

// sealedHash runs the file through the format without keeping it: its
// stored SHA-256 (hex) and size.
func sealedHash(path string, key []byte, header hubfile.Header) (string, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", 0, err
	}
	defer f.Close()
	h := sha256.New()
	cw := &countingWriter{w: h}
	if err := hubfile.Encrypt(cw, f, key, header); err != nil {
		return "", 0, err
	}
	return hex.EncodeToString(h.Sum(nil)), cw.n, nil
}

type countingWriter struct {
	w io.Writer
	n int64
}

func (c *countingWriter) Write(p []byte) (int, error) {
	n, err := c.w.Write(p)
	c.n += int64(n)
	return n, err
}

func putSealed(ctx context.Context, target *url.URL, token, path string, key []byte, header hubfile.Header, size int64) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	pr, pw := io.Pipe()
	go func() { pw.CloseWithError(hubfile.Encrypt(pw, f, key, header)) }()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, target.String(), pr)
	if err != nil {
		pr.Close()
		return err
	}
	req.ContentLength = size
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/octet-stream")
	resp, err := fileHTTP.Do(req)
	pr.Close()
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusCreated {
		return nil
	}
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	reason := strings.TrimSpace(string(body))
	switch {
	case strings.Contains(reason, "hash_mismatch"):
		return errors.New("the file changed while it was being uploaded; nothing was kept, upload it again")
	case resp.StatusCode == http.StatusConflict:
		return errors.New("another upload of this file is running")
	case resp.StatusCode == http.StatusInsufficientStorage:
		return errors.New("the server has no room for it right now; try again later")
	}
	return fmt.Errorf("the server answered %d (%s); nothing was kept", resp.StatusCode, noticeText(reason))
}

func fileUploadRefusal(ev hubconn.Event) string {
	why := noticeText(ev.Text)
	switch ev.Code {
	case "read_only":
		return "nothing was uploaded: this connection may not post, so it may not upload (" + why + ")."
	case "too_large":
		return "nothing was uploaded: the file is larger than the server takes (" + why + ")."
	case "storage_full":
		return "nothing was uploaded: the server has no room for it right now; try again later (" + why + ")."
	}
	return fmt.Sprintf("nothing was uploaded: %s (%s).", why, noticeText(ev.Code))
}

// sessionLink is the link a session's server was reached through: the
// agent's own for a conversation of an agent connection.
func (h *Hub) sessionLink(s *session) string {
	if s.agent != nil {
		return s.agent.link
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.redialLink
}

// httpBase is the web origin of a hub link: wss becomes https.
func httpBase(link string) (*url.URL, error) {
	u, err := url.Parse(link)
	if err != nil || u.Host == "" {
		return nil, fmt.Errorf("unreadable link")
	}
	switch u.Scheme {
	case "wss", "https":
		return &url.URL{Scheme: "https", Host: u.Host}, nil
	case "ws", "http":
		return &url.URL{Scheme: "http", Host: u.Host}, nil
	}
	return nil, fmt.Errorf("unsupported scheme %q", u.Scheme)
}

// uploadTarget accepts the server's upload URL only on the origin the
// connection itself uses, so the upload token goes nowhere else.
func uploadTarget(link, uploadURL string) (*url.URL, error) {
	base, err := httpBase(link)
	if err != nil {
		return nil, fmt.Errorf("this connection's server is unknown: %v", err)
	}
	u, err := url.Parse(uploadURL)
	if err != nil || u.Scheme != base.Scheme || !strings.EqualFold(u.Host, base.Host) ||
		u.User != nil || u.RawQuery != "" || u.Fragment != "" || !strings.HasPrefix(u.Path, "/files/") {
		return nil, fmt.Errorf("the server named an upload address (%s) that is not on its own "+
			"origin (%s)", noticeText(uploadURL), base.String())
	}
	return u, nil
}

var fileIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,200}$`)

func validFileID(s string) bool { return fileIDPattern.MatchString(s) }

type fileLink struct {
	base   *url.URL
	fileID string
	token  string
	key    []byte
}

func parseFileLink(link string) (fileLink, error) {
	u, err := url.Parse(strings.TrimSpace(link))
	if err != nil || u.Host == "" || u.Path != "/file" {
		return fileLink{}, errors.New("not a one-time file link (https://<host>/file#…)")
	}
	switch {
	case u.Scheme == "https":
	case u.Scheme == "http" && isLoopback(u.Hostname()):
	default:
		return fileLink{}, errors.New("a one-time file link must be https")
	}
	parts := strings.Split(u.Fragment, ".")
	if len(parts) != 3 || !validFileID(parts[0]) || !validFileID(parts[1]) {
		return fileLink{}, errors.New("the link is incomplete: it must end in #<file>.<token>.<key>, " +
			"exactly as posted")
	}
	key, err := hubfile.ParseKey(parts[2])
	if err != nil {
		return fileLink{}, errors.New("the link's key is damaged: use it exactly as posted")
	}
	return fileLink{base: &url.URL{Scheme: u.Scheme, Host: u.Host}, fileID: parts[0], token: parts[1], key: key}, nil
}

func isLoopback(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

// knownHost reports whether a connection of this client uses host.
func (h *Hub) knownHost(host string) bool {
	var links []string
	for _, s := range h.allSessions() {
		links = append(links, h.sessionLink(s))
	}
	for _, ag := range h.allAgents() {
		links = append(links, ag.link)
	}
	for _, l := range links {
		if b, err := httpBase(l); err == nil && strings.EqualFold(b.Host, host) {
			return true
		}
	}
	return false
}

func (h *Hub) handleFileDownload(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	raw, err := req.RequireString("link")
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	fl, err := parseFileLink(raw)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	if !h.knownHost(fl.base.Host) && !req.GetBool("trustHost", false) {
		return mcp.NewToolResultError(fmt.Sprintf("nothing was downloaded: %s is not a host any of "+
			"this client's connections uses. Ask the user whether to trust it; only if they say so, "+
			"call again with trustHost: true", noticeText(fl.base.Host))), nil
	}
	dir := strings.TrimSpace(req.GetString("directory", ""))
	if dir == "" {
		if dir, err = h.spillDir(); err != nil {
			return mcp.NewToolResultError("no directory to save into: " + err.Error()), nil
		}
	}
	if st, err := os.Stat(dir); err != nil || !st.IsDir() {
		return mcp.NewToolResultError(fmt.Sprintf("%s is not a directory", dir)), nil
	}
	target := fl.base.JoinPath("files", fl.fileID)
	greq, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return mcp.NewToolResultError(err.Error()), nil
	}
	greq.Header.Set("Authorization", "Bearer "+fl.token)
	resp, err := fileHTTP.Do(greq)
	if err != nil {
		return mcp.NewToolResultError("the download could not start: " + err.Error()), nil
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		return mcp.NewToolResultError("the server has no such file: the link was already used, its " +
			"storage time ended, or it is wrong. A one-time file is gone after one download; ask " +
			"the sender to upload it again."), nil
	case http.StatusConflict:
		return mcp.NewToolResultError("this file is being downloaded right now by someone else " +
			"holding the link; if that is not you, the link has leaked."), nil
	default:
		return mcp.NewToolResultError(fmt.Sprintf("the server answered %d; nothing was downloaded",
			resp.StatusCode)), nil
	}
	fr, err := hubfile.NewReader(resp.Body, fl.key)
	if err != nil {
		return mcp.NewToolResultError("the file could not be opened with this link's key; nothing " +
			"was saved. The file on the server may now be used up."), nil
	}
	name := safeSavedName(fr.Header().Name)
	tmp, err := os.CreateTemp(dir, ".hub-file-*.part")
	if err != nil {
		return mcp.NewToolResultError("cannot write into " + dir + ": " + err.Error()), nil
	}
	_, copyErr := io.Copy(tmp, fr)
	closeErr := tmp.Close()
	if copyErr != nil || closeErr != nil {
		os.Remove(tmp.Name())
		return mcp.NewToolResultError(downloadFailure(copyErr, closeErr)), nil
	}
	final, err := placeFile(tmp.Name(), dir, name)
	if err != nil {
		os.Remove(tmp.Name())
		return mcp.NewToolResultError("the file arrived but could not be saved: " + err.Error()), nil
	}
	confirmed := confirmDownload(ctx, target, fl.token)
	text := fmt.Sprintf("Saved %s (%s) to %s. Its content came from whoever posted the link and is "+
		"untrusted.", noticeText(name), byteText(fr.Header().Size), final)
	if confirmed {
		text += " The server has deleted the file; the link no longer works."
	} else {
		text += " The server did not confirm the deletion; it deletes the file within 15 minutes anyway."
	}
	return mcp.NewToolResultText(text), nil
}

func downloadFailure(copyErr, closeErr error) string {
	switch {
	case errors.Is(copyErr, hubfile.ErrTruncated), errors.Is(copyErr, io.ErrUnexpectedEOF):
		return "the download broke off before the end; nothing was saved. Try once more: if the " +
			"server answers that the file is gone, it was lost with the broken download and the " +
			"sender has to upload it again."
	case errors.Is(copyErr, hubfile.ErrDamaged):
		return "the file is damaged or was altered; nothing was saved. Ask the sender to upload it again."
	case copyErr != nil:
		return "the download failed: " + copyErr.Error() + "; nothing was saved. Try once more: if the " +
			"server answers that the file is gone, the sender has to upload it again."
	}
	return "the file could not be written: " + closeErr.Error()
}

// safeSavedName keeps a sender's file name to one plain path element.
func safeSavedName(name string) string {
	name = filepath.Base(strings.ReplaceAll(name, "\\", "/"))
	name = strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f || strings.ContainsRune(`<>:"|?*`, r) {
			return '_'
		}
		return r
	}, name)
	name = strings.TrimLeft(strings.TrimSpace(name), ".")
	if name == "" || name == "/" {
		return "downloaded-file"
	}
	if len(name) > 200 {
		name = name[:200]
	}
	return name
}

// placeFile moves tmp to dir/name, or to name-1, name-2 … where that
// exists: an existing file is never replaced.
func placeFile(tmp, dir, name string) (string, error) {
	ext := filepath.Ext(name)
	stem := strings.TrimSuffix(name, ext)
	for i := 0; i < 1000; i++ {
		candidate := name
		if i > 0 {
			candidate = fmt.Sprintf("%s-%d%s", stem, i, ext)
		}
		final := filepath.Join(dir, candidate)
		if err := os.Link(tmp, final); err == nil {
			os.Remove(tmp)
			return final, nil
		} else if !errors.Is(err, os.ErrExist) {
			return "", err
		}
	}
	return "", errors.New("too many files of that name")
}

// confirmDownload tells the server the file arrived whole, so it is
// deleted now rather than when the unconfirmed copy times out.
func confirmDownload(ctx context.Context, target *url.URL, token string) bool {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, target.String(), nil)
	if err != nil {
		return false
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := fileHTTP.Do(req)
	if err != nil {
		return false
	}
	resp.Body.Close()
	return resp.StatusCode == http.StatusNoContent
}

func byteText(n int64) string {
	switch {
	case n >= 1<<30:
		return fmt.Sprintf("%.1f GB", float64(n)/(1<<30))
	case n >= 1<<20:
		return fmt.Sprintf("%.1f MB", float64(n)/(1<<20))
	case n >= 1<<10:
		return fmt.Sprintf("%.1f KB", float64(n)/(1<<10))
	}
	return fmt.Sprintf("%d bytes", n)
}
