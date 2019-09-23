package recognize

type FaceInfo struct {
	Id         string       `json:"id"`
	Label      string       `json:"label"`
	Descriptor [128]float32 `json:"descriptor"`
	MD5        string       `json:"md5"`
}
