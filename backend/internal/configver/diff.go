package configver

import "strings"

// DiffLine is one line of a unified-style diff. Op is " " (context), "+"
// (added), or "-" (removed).
type DiffLine struct {
	Op   string `json:"op"`
	Text string `json:"text"`
}

// Diff returns a line-level diff between two config YAML versions, computed from
// the longest common subsequence so only genuinely changed lines are marked —
// the git-style view the UI renders between versions.
func Diff(oldYAML, newYAML string) []DiffLine {
	a := splitLines(oldYAML)
	b := splitLines(newYAML)

	// LCS length table.
	m, n := len(a), len(b)
	lcs := make([][]int, m+1)
	for i := range lcs {
		lcs[i] = make([]int, n+1)
	}
	for i := m - 1; i >= 0; i-- {
		for j := n - 1; j >= 0; j-- {
			if a[i] == b[j] {
				lcs[i][j] = lcs[i+1][j+1] + 1
			} else if lcs[i+1][j] >= lcs[i][j+1] {
				lcs[i][j] = lcs[i+1][j]
			} else {
				lcs[i][j] = lcs[i][j+1]
			}
		}
	}

	// Walk the table to emit context/removed/added lines in order.
	var out []DiffLine
	i, j := 0, 0
	for i < m && j < n {
		switch {
		case a[i] == b[j]:
			out = append(out, DiffLine{" ", a[i]})
			i++
			j++
		case lcs[i+1][j] >= lcs[i][j+1]:
			out = append(out, DiffLine{"-", a[i]})
			i++
		default:
			out = append(out, DiffLine{"+", b[j]})
			j++
		}
	}
	for ; i < m; i++ {
		out = append(out, DiffLine{"-", a[i]})
	}
	for ; j < n; j++ {
		out = append(out, DiffLine{"+", b[j]})
	}
	return out
}

// Changed reports whether a diff contains any add/remove (i.e. the versions
// differ).
func Changed(d []DiffLine) bool {
	for _, l := range d {
		if l.Op != " " {
			return true
		}
	}
	return false
}

func splitLines(s string) []string {
	s = strings.TrimRight(s, "\n")
	if s == "" {
		return nil
	}
	return strings.Split(s, "\n")
}
