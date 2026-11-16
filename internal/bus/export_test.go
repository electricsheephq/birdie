package bus

import "time"

// Asleep exposes asleep to the tests.
var Asleep = asleep

// Woke exposes Bus.woke to the tests.
func (b *Bus) Woke(last, now time.Time) { b.woke(last, now) }
