package sort

import (
    "math/rand"
    "time"
)

func Prepare(src []int64) {
    rand.Seed(time.Now().Unix())
    for i := range src {
        src[i] = rand.Int63()
    }
}
