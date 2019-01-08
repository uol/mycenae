package oi


import (
	"github.com/reiver/go-oi/test"

	"testing"
)


func TestLongWriteByte(t *testing.T) {

	tests := []struct{
		Byte byte
	}{}

	for b := byte(' '); b <= byte('/'); b++ {
		test := struct {
			Byte byte
		}{
			Byte:b,
		}

		tests = append(tests, test)
	}

	for b := byte('0'); b <= byte('9'); b++ {
		test := struct {
			Byte byte
		}{
			Byte:b,
		}

		tests = append(tests, test)
	}

	for b := byte(':'); b <= byte('@'); b++ {
		test := struct {
			Byte byte
		}{
			Byte:b,
		}

		tests = append(tests, test)
	}

	for b := byte('A'); b <= byte('Z'); b++ {
		test := struct {
			Byte byte
		}{
			Byte:b,
		}

		tests = append(tests, test)
	}

	for b := byte('['); b <= byte('`'); b++ {
		test := struct {
			Byte byte
		}{
			Byte:b,
		}

		tests = append(tests, test)
	}

	for b := byte('a'); b <= byte('z'); b++ {
		test := struct {
			Byte byte
		}{
			Byte:b,
		}

		tests = append(tests, test)
	}

	for b := byte('{'); b <= byte('~'); b++ {
		test := struct {
			Byte byte
		}{
			Byte:b,
		}

		tests = append(tests, test)
	}


	for testNumber, test := range tests {

		var writer oitest.ShortWriter
		err := LongWriteByte(&writer, test.Byte)
		if nil != err {
			t.Errorf("For test #%d, did not expect an error, but actually got one: (%T) %q; for %d (%q).", testNumber, err, err.Error(), test.Byte, string(test.Byte))
			continue
		}
		if expected, actual := string(test.Byte), writer.String(); expected != actual {
			t.Errorf("For test #%d, expected %q, but actually got %q", testNumber, expected, actual)
			continue
		}
	}
}
