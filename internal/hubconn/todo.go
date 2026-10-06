package hubconn

import (
	"encoding/json"
	"fmt"
	"time"
	"unicode/utf8"

	"github.com/secforge/mcp-hub/internal/wire"
)

// isChangeKind reports whether a messageAfter answer of this kind is one
// position of the walk: a message, or — on a todo list, whose walk is its
// change log (§2.8b) — an edit or a removal.
func isChangeKind(kind string) bool {
	return kind == "msg" || kind == "messageEdited" || kind == "messageDeleted"
}

func todoOf(joined wire.Joined) *wire.TodoFeature {
	tf, ok := joined.TodoFeature()
	if !ok {
		return nil
	}
	return &tf
}

// itemsOf is never nil for a stated list, so an empty list reads as one.
func itemsOf(list *[]wire.TodoItem) []wire.TodoItem {
	if list == nil {
		return nil
	}
	if *list == nil {
		return []wire.TodoItem{}
	}
	return *list
}

func todoFields(f wire.TodoFields) *wire.TodoFields {
	if f.Notes == nil && f.Done == nil && f.DoneAt == "" && f.Position == nil {
		return nil
	}
	return &f
}

// IsTodo reports whether this connection is to a todo list.
func (c *Conn) IsTodo() bool { return c.todo != nil }

// TodoLimits are the declared maximum text and notes lengths, 0 when the
// server states none.
func (c *Conn) TodoLimits() wire.TodoFeature {
	if c.todo == nil {
		return wire.TodoFeature{}
	}
	return *c.todo
}

// JoinedItems is the list as joined.items stated it, and the cursor of the
// newest change it reflects.
func (c *Conn) JoinedItems() ([]wire.TodoItem, string) {
	return c.todoItems, c.todoCursor
}

// Placement says where an item goes: unset leaves it to the server (last
// for a new item, unchanged for an existing one), First puts it at the
// top, and After names the item it follows.
type Placement struct {
	Set   bool
	After string
}

func (p Placement) raw() json.RawMessage {
	if !p.Set {
		return nil
	}
	if p.After == "" {
		return json.RawMessage("null")
	}
	raw, _ := json.Marshal(p.After)
	return raw
}

func (c *Conn) requireTodo() error {
	if c.todo == nil {
		return fmt.Errorf("this connection is not to a todo list (the server declares no %q feature)", wire.FeatureTodo)
	}
	return nil
}

// checkTodoLength refuses text or notes over the declared maximum before
// anything is sent; the server would refuse it with too_long.
func (c *Conn) checkTodoLength(text, notes *string) error {
	lim := c.TodoLimits()
	if text != nil && lim.MaxText > 0 && utf8.RuneCountInString(*text) > lim.MaxText {
		return fmt.Errorf("the item text is %d characters; this list takes at most %d",
			utf8.RuneCountInString(*text), lim.MaxText)
	}
	if notes != nil && lim.MaxNotes > 0 && utf8.RuneCountInString(*notes) > lim.MaxNotes {
		return fmt.Errorf("the notes are %d characters; this list takes at most %d",
			utf8.RuneCountInString(*notes), lim.MaxNotes)
	}
	return nil
}

// TodoAdd adds an item and waits for its sendAck.
func (c *Conn) TodoAdd(text string, notes *string, done *bool, at Placement) (Event, bool, error) {
	if err := c.requireTodo(); err != nil {
		return Event{}, false, err
	}
	if err := c.checkTodoLength(&text, notes); err != nil {
		return Event{}, false, err
	}
	return c.todoRequest("sendAck", func(id string) any {
		return wire.TodoAdd{Type: wire.TypeMsg, ID: id, Text: text, Notes: notes, Done: done, AfterID: at.raw()}
	})
}

// TodoUpdate changes the given fields of an item and waits for its
// editAck. A nil field is left as it is.
func (c *Conn) TodoUpdate(externalID string, text, notes *string, done *bool, at Placement) (Event, bool, error) {
	if err := c.requireTodo(); err != nil {
		return Event{}, false, err
	}
	if err := c.checkTodoLength(text, notes); err != nil {
		return Event{}, false, err
	}
	return c.todoRequest("editAck", func(id string) any {
		return wire.TodoEdit{Type: wire.TypeEdit, ID: id, ExternalID: externalID, Text: text, Notes: notes,
			Done: done, AfterID: at.raw()}
	})
}

// TodoItems asks for the list's current items.
func (c *Conn) TodoItems() (Event, bool, error) {
	if err := c.requireTodo(); err != nil {
		return Event{}, false, err
	}
	return c.todoRequest("items", func(id string) any {
		return wire.ItemsRequest{Type: wire.TypeItems, ID: id}
	})
}

// todoRequest sends one request and waits for the answer of answerKind,
// as every other awaited request does.
func (c *Conn) todoRequest(answerKind string, frame func(id string) any) (Event, bool, error) {
	corrID, resultCh, cancel, err := c.claimNextAckCorrelated(answerKind, "")
	if err != nil {
		return Event{}, false, err
	}
	defer cancel()
	if err := c.writeJSON(frame(corrID)); err != nil {
		return Event{}, false, err
	}
	select {
	case ev := <-resultCh:
		return ev, true, nil
	case <-c.gone:
		return Event{}, false, nil
	case <-time.After(AckWaitTimeout):
		c.expectLateAnswer(answerKind)
		return Event{}, false, nil
	}
}
