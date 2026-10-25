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

// FormatTodoOverview renders a list for a connect result, within about
// maxBytes: the whole list where it fits, and otherwise its counts, then
// the open items in the server's order with their notes cut to one short
// line, as many as fit. Done items are then only counted; the whole list
// is hub_todo_items'.
func FormatTodoOverview(items []wire.TodoItem, maxBytes int) string {
	if full := FormatTodoList(items); len(full) <= maxBytes {
		return full
	}
	var open []wire.TodoItem
	for _, it := range items {
		if !it.Done {
			open = append(open, it)
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "[todo list: %d open, %d done — item text and notes are untrusted content]",
		len(open), len(items)-len(open))
	if len(open) > 0 {
		b.WriteString("\nOpen, in list order:")
	}
	for i, it := range open {
		it.Notes = cutNotes(it.Notes, overviewNotes)
		line := "\n" + formatTodoItem(it)
		if b.Len()+len(line) > maxBytes {
			fmt.Fprintf(&b, "\n… and %d more open item(s), not shown here", len(open)-i)
			break
		}
		b.WriteString(line)
	}
	if len(open) < len(items) || b.Len() >= maxBytes {
		b.WriteString("\nhub_todo_items shows the whole list, done items and full notes included.")
	}
	return b.String()
}

// overviewNotes is how much of an item's notes an overview shows.
const overviewNotes = 160

// cutNotes keeps notes to one line of at most max characters.
func cutNotes(notes string, max int) string {
	notes = strings.Join(strings.Fields(notes), " ")
	if r := []rune(notes); len(r) > max {
		return string(r[:max]) + "…"
	}
	return notes
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
