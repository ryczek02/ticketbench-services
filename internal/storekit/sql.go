package storekit

import "strings"

// SQLStatements strips "--" comment lines from an SQL script and splits it
// on semicolons.
func SQLStatements(sql string) []string {
	var keep []string
	for _, l := range strings.Split(sql, "\n") {
		if t := strings.TrimSpace(l); t != "" && !strings.HasPrefix(t, "--") {
			keep = append(keep, l)
		}
	}
	var out []string
	for _, s := range strings.Split(strings.Join(keep, "\n"), ";") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}
