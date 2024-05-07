package requests

type Text struct {
	PointTransactionType    	string  `json:"PointTransactionType"`
	Language          			string  `json:"Language"`
	PointTransactionTypeName	string  `json:"PointTransactionTypeName"`
	CreationDate				string	`json:"CreationDate"`
	LastChangeDate				string	`json:"LastChangeDate"`
	IsMarkedForDeletion			*bool	`json:"IsMarkedForDeletion"`
}
