package dpfm_api_output_formatter

import (
	"data-platform-api-point-transaction-type-reads-rmq-kube/DPFM_API_Caller/requests"
	"database/sql"
	"fmt"
)

func ConvertToPointTransactionType(rows *sql.Rows) (*[]PointTransactionType, error) {
	defer rows.Close()
	pointTransactionType := make([]PointTransactionType, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.PointTransactionType{}

		err := rows.Scan(
			&pm.PointTransactionType,
			&pm.CreationDate,
			&pm.LastChangeDate,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &pointTransactionType, nil
		}

		data := pm
		pointTransactionType = append(pointTransactionType, PointTransactionType{
			PointTransactionType:	data.PointTransactionType,
			CreationDate:			data.CreationDate,
			LastChangeDate:			data.LastChangeDate,
			IsMarkedForDeletion:	data.IsMarkedForDeletion,
		})
	}

	return &pointTransactionType, nil
}

func ConvertToText(rows *sql.Rows) (*[]Text, error) {
	defer rows.Close()
	text := make([]Text, 0)

	i := 0
	for rows.Next() {
		i++
		pm := &requests.Text{}

		err := rows.Scan(
			&pm.PointTransactionType,
			&pm.Language,
			&pm.PointTransactionTypeName,
			&pm.CreationDate,
			&pm.LastChangeDate,
			&pm.IsMarkedForDeletion,
		)
		if err != nil {
			fmt.Printf("err = %+v \n", err)
			return &text, err
		}

		data := pm
		text = append(text, Text{
			PointTransactionType:		data.PointTransactionType,
			Language:          			data.Language,
			PointTransactionTypeName:	data.PointTransactionTypeName,
			CreationDate:				data.CreationDate,
			LastChangeDate:				data.LastChangeDate,
			IsMarkedForDeletion:		data.IsMarkedForDeletion,
		})
	}

	return &text, nil
}
