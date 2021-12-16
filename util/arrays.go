package util

func IndexOf(arr []string, v string) int {
    for i, a := range arr {
        if a == v {
            return i
        }
    }
    return -1
}
