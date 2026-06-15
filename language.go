package atmos

import "regexp"

// Language represents a well-formed BCP-47 language tag.
type Language string

// bcp47Regexp validates well-formed BCP-47 syntax (RFC 5646 §2.1). It is ported
// verbatim from the reference @atproto/syntax implementation so that atmos
// accepts exactly the same set of tags as the canonical stack — including
// private-use tags (x-…), 4–8 character primary subtags, and the closed set of
// grandfathered tags. Go's regexp uses (?P<name>) for named groups; the pattern
// is otherwise identical to the reference.
var bcp47Regexp = regexp.MustCompile(`^((?P<grandfathered>(en-GB-oed|i-ami|i-bnn|i-default|i-enochian|i-hak|i-klingon|i-lux|i-mingo|i-navajo|i-pwn|i-tao|i-tay|i-tsu|sgn-BE-FR|sgn-BE-NL|sgn-CH-DE)|(art-lojban|cel-gaulish|no-bok|no-nyn|zh-guoyu|zh-hakka|zh-min|zh-min-nan|zh-xiang))|((?P<language>([A-Za-z]{2,3}(-(?P<extlang>[A-Za-z]{3}(-[A-Za-z]{3}){0,2}))?)|[A-Za-z]{4}|[A-Za-z]{5,8})(-(?P<script>[A-Za-z]{4}))?(-(?P<region>[A-Za-z]{2}|[0-9]{3}))?(-(?P<variant>[A-Za-z0-9]{5,8}|[0-9][A-Za-z0-9]{3}))*(-(?P<extension>[0-9A-WY-Za-wy-z](-[A-Za-z0-9]{2,8})+))*(-(?P<privateUseA>x(-[A-Za-z0-9]{1,8})+))?)|(?P<privateUseB>x(-[A-Za-z0-9]{1,8})+))$`)

// ParseLanguage validates raw as a well-formed BCP-47 language tag and returns
// it as a Language.
func ParseLanguage(raw string) (Language, error) {
	if len(raw) == 0 {
		return "", syntaxErr("Language", raw, "empty")
	}
	if !bcp47Regexp.MatchString(raw) {
		return "", syntaxErr("Language", raw, "not a well-formed BCP-47 language tag")
	}
	return Language(raw), nil
}

func (l Language) String() string {
	return string(l)
}

func (l Language) MarshalText() ([]byte, error) {
	return []byte(l), nil
}

func (l *Language) UnmarshalText(b []byte) error {
	parsed, err := ParseLanguage(string(b))
	if err != nil {
		return err
	}
	*l = parsed
	return nil
}
