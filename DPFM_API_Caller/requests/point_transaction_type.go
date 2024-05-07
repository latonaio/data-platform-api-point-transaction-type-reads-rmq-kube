package requests

type PointTransactionType struct {
	PointTransactionType	string	`json:"PointTransactionType"`
	CreationDate			string	`json:"CreationDate"`
	LastChangeDate			string	`json:"LastChangeDate"`
	IsMarkedForDeletion		*bool	`json:"IsMarkedForDeletion"`
}
