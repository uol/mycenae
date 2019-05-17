package oi


import (
	"github.com/reiver/go-oi/test"

	"errors"
	"testing"
)


func TestLongWriteString(t *testing.T) {

	tests := []struct{
		String string
	}{
		{
			String: "",
		},



		{
			String: "apple",
		},
		{
			String: "banana",
		},
		{
			String: "cherry",
		},



		{
			String: "Hello world!",
		},



		{
			String: "😁😂😃😄😅😆😉😊😋😌😍😏😒😓😔😖😘😚😜😝😞😠😡😢😣😤😥😨😩😪😫😭😰😱😲😳😵😷",
		},



		{
			String: "0123456789",
		},
		{
			String: "٠١٢٣٤٥٦٧٨٩", // Arabic-Indic Digits
		},
		{
			String: "۰۱۲۳۴۵۶۷۸۹", // Extended Arabic-Indic Digits
		},



		{
			String: "Ⅰ Ⅱ Ⅲ Ⅳ Ⅴ Ⅵ Ⅶ Ⅷ Ⅸ Ⅹ Ⅺ Ⅻ Ⅼ Ⅽ Ⅾ Ⅿ",
		},
		{
			String: "ⅰ ⅱ ⅲ ⅳ ⅴ ⅵ ⅶ ⅷ ⅸ ⅹ ⅺ ⅻ ⅼ ⅽ ⅾ ⅿ",
		},
		{
			String: "ↀ ↁ ↂ Ↄ ↄ ↅ ↆ ↇ ↈ",
		},
	}


	for testNumber, test := range tests {

		var writer oitest.ShortWriter
		n, err := LongWriteString(&writer, test.String)
		if nil != err {
			t.Errorf("For test #%d, did not expect an error, but actually got one: (%T) %q; for %q.", testNumber, err, err.Error(), test.String)
			continue
		}
		if expected, actual := int64(len([]byte(test.String))), n; expected != actual {
			t.Errorf("For test #%d, expected %d, but actually got %d; for %q.", testNumber, expected, actual, test.String)
			continue
		}
		if expected, actual := test.String, writer.String(); expected != actual {
			t.Errorf("For test #%d, expected %q, but actually got %q", testNumber, expected, actual)
			continue
		}
	}
}


func TestLongWriteStringExpectError(t *testing.T) {

	tests := []struct{
		String     string
		Expected   string
		Writes   []int
		Err        error
	}{
		{
			String:   "apple",
			Expected: "appl",
			Writes: []int{2,2},
			Err: errors.New("Crabapple!"),
		},
		{
			String:   "apple",
			Expected: "appl",
			Writes: []int{2,2,0},
			Err: errors.New("Crabapple!!"),
		},



		{
			String: "banana",
			Expected: "banan",
			Writes: []int{2,3},
			Err: errors.New("bananananananana!"),
		},
		{
			String: "banana",
			Expected: "banan",
			Writes: []int{2,3,0},
			Err: errors.New("bananananananananananananana!!!"),
		},



		{
			String: "cherry",
			Expected: "cher",
			Writes: []int{1,1,1,1},
			Err: errors.New("C.H.E.R.R.Y."),
		},
		{
			String: "cherry",
			Expected: "cher",
			Writes: []int{1,1,1,1,0},
			Err: errors.New("C_H_E_R_R_Y"),
		},



		{
			String: "Hello world!",
			Expected: "Hello world",
			Writes: []int{1,2,3,5},
			Err: errors.New("Welcome!"),
		},
		{
			String: "Hello world!",
			Expected: "Hello world",
			Writes: []int{1,2,3,5,0},
			Err: errors.New("WeLcOmE!!!"),
		},



		{
			String: "                                      ",
			Expected: "                                ",
			Writes: []int{1,2,3,5,8,13},
			Err: errors.New("Space, the final frontier"),
		},
		{
			String: "                                      ",
			Expected: "                                ",
			Writes: []int{1,2,3,5,8,13,0},
			Err: errors.New("Space, the final frontier"),
		},
	}

	for testNumber, test := range tests {

		writer := oitest.NewWritesThenErrorWriter(test.Err, test.Writes...)
		n, err := LongWriteString(writer, test.String)
		if nil == err {
			t.Errorf("For test #%d, expected to get an error, but actually did not get one: %v; for %q.", testNumber, err, test.String)
			continue
		}
		if expected, actual := test.Err, err; expected != actual {
			t.Errorf("For test #%d, expected to get error (%T) %q, but actually got (%T) %q; for %q.", testNumber, expected, expected.Error(), actual, actual.Error(), test.String)
			continue
		}
		if expected, actual := int64(len(test.Expected)), n; expected != actual {
			t.Errorf("For test #%d, expected number of bytes written to be %d = len(%q), but actually was %d = len(%q); for %q.", testNumber, expected, test.Expected, actual, writer.String(), test.String)
			continue
		}
	}

}
