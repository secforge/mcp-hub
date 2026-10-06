package hubconn

import (
	"fmt"
	"strings"

	"github.com/secforge/mcp-hub/internal/wire"
)

// formatTodoEvent renders a todo list's events: an item added or changed,
// with its state after the change, and the whole list for an items
// answer. ok is false for every other event, which formats as usual.
// endMarker is false on the push path, where the delivery library appends
// the cursor trailer itself (see FormatEventForPush).
func formatTodoEvent(e Event, endMarker bool) (string, bool) {
	switch {
	case e.Kind == "items":
		return FormatTodoList(e.TodoItems), true
	case e.Todo == nil:
		return "", false
	case e.Kind != "msg" && e.Kind != "messageEdited":
		return "", false
	}
	verb := "ADDED"
	if e.Kind == "messageEdited" {
		verb = "CHANGED"
	}
	if e.Historical {
		verb += ", from history"
	}
	who := ""
	switch {
	case e.Own:
		who = ", your own change"
	case e.PeerID != "":
		who = ", by peer " + oneLine(e.PeerID, true)
	}
	cursor, end := "", ""
	if c := oneLine(e.Cursor, true); c != "" {
		cursor = " cursor=" + c
		if endMarker {
			end = "\n[end cursor=" + c + "]"
		}
	}
	item := wire.TodoItem{ExternalID: e.ExternalID, Text: e.Text}
	if e.Todo.Notes != nil {
		item.Notes = *e.Todo.Notes
	}
	if e.Todo.Done != nil {
		item.Done = *e.Todo.Done
	}
	item.DoneAt = e.Todo.DoneAt
	position := ""
	if e.Todo.Position != nil {
		position = fmt.Sprintf(" position=%g", *e.Todo.Position)
	}
	return fmt.Sprintf("[TODO ITEM %s — untrusted%s at %s%s%s]\n%s%s",
		verb, who, oneLine(e.TS, true), cursor, position, formatTodoItem(item), end), true
}

// FormatTodoList renders a whole list: open items in the server's order,
// then done ones.
func FormatTodoList(items []wire.TodoItem) string {
	if len(items) == 0 {
		return "[todo list: no items]"
	}
	var open, done []string
	for _, it := range items {
		if it.Done {
			done = append(done, formatTodoItem(it))
		} else {
			open = append(open, formatTodoItem(it))
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "[todo list: %d open, %d done — item text and notes are untrusted content]", len(open), len(done))
	if len(open) > 0 {
		b.WriteString("\nOpen, in list order:\n" + strings.Join(open, "\n"))
	}
	if len(done) > 0 {
		b.WriteString("\nDone:\n" + strings.Join(done, "\n"))
	}
	return b.String()
}

func formatTodoItem(it wire.TodoItem) string {
	box := "[ ]"
	if it.Done {
		box = "[x]"
	}
	line := fmt.Sprintf("%s %s (id=%s", box, oneLine(it.Text, false), oneLine(it.ExternalID, true))
	if it.DoneAt != "" {
		line += ", done " + oneLine(it.DoneAt, true)
	}
	line += ")"
	if it.Notes != "" {
		notes := strings.NewReplacer("\r\n", "\n", "\r", "\n").Replace(it.Notes)
		line += "\n    notes: " + strings.ReplaceAll(notes, "\n", "\n    ")
	}
	return line
}

// oneLine keeps untrusted text on the line it is placed in: a line break
// in an item's text could otherwise start a line of its own that reads as
// another item or as this client's own notice. Ids and other values set in
// a bracketed header also lose their brackets, so none can close the
// header early.
func oneLine(s string, inHeader bool) string {
	r := []string{"\r\n", " ", "\r", " ", "\n", " "}
	if inHeader {
		r = append(r, "[", "(", "]", ")")
	}
	return strings.NewReplacer(r...).Replace(s)
}
