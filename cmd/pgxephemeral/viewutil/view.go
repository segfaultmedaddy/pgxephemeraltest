package viewutil

import (
	"strconv"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"go.segfaultmedaddy.com/pgxephemeraltest/internal/dbmanager"
)

const (
	FormatText = "text"
	FormatJSON = "json"
)

func BoolStr(b bool) string {
	if b {
		return "Yes"
	}

	return "No"
}

type TableView struct {
	table table.Model
}

func NewTableView(dbs []dbmanager.DBInfo) *TableView {
	columns := []table.Column{
		{Title: "No", Width: 5},
		{Title: "Name", Width: 50},
		{Title: "Template", Width: 10},
	}

	rows := make([]table.Row, len(dbs))
	for i, db := range dbs {
		rows[i] = table.Row{strconv.Itoa(i + 1), db.Name, BoolStr(db.IsTemplate)}
	}

	t := table.New(
		table.WithColumns(columns),
		table.WithRows(rows),
		table.WithFocused(true),
		table.WithWidth(65),
		table.WithHeight(20),
	)

	s := table.DefaultStyles()
	s.Header = s.Header.
		Border(lipgloss.NormalBorder(), false, false, true, false).
		Bold(true)
	t.SetStyles(s)

	return &TableView{table: t}
}

func (m TableView) Init() tea.Cmd {
	return nil
}

func (m TableView) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.table.SetHeight(msg.Height - 4)
	case tea.KeyMsg:
		switch msg.String() {
		case "q", "ctrl+c", "esc":
			return m, tea.Quit
		}
	}

	var cmd tea.Cmd

	m.table, cmd = m.table.Update(msg)

	return m, cmd
}

func (m TableView) View() tea.View {
	return tea.NewView(m.table.View() + "\n" + m.table.HelpView() + "\n")
}
