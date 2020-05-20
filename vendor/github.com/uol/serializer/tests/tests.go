package tests

import "math/rand"

// GenerateRandom - generates a random number
func GenerateRandom(min, max int) int {

	return (rand.Intn(max-min+1) + min)
}
