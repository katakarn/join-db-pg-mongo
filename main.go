package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gocarina/gocsv"
	_ "github.com/lib/pq"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type MongoData struct {
	ID            string    `bson:"_id" csv:"mongo_id"`
	VoucherID     string    `bson:"voucherId" csv:"voucher_id"`
	SubCode       string    `bson:"subCode" csv:"sub_code"`
	RefCode       string    `bson:"refCode" csv:"ref_code"`
	QRCode        string    `bson:"qrCode" csv:"qr_code"`
	CollectedDate time.Time `bson:"collectedDate" csv:"collected_date"`
	UsedDate      time.Time `bson:"usedDate" csv:"used_date"`
	Status        string    `bson:"status" csv:"status"`
	CutOffStatus  string    `bson:"cutOffStatus" csv:"cut_off_status"`
	CutOffDate    time.Time `bson:"cutOffDate" csv:"cut_off_date"`
	StudentID     int       `bson:"studentId" csv:"student_id"`
	Email         string    `bson:"email" csv:"email"`
	StudentName   string    `bson:"studentName" csv:"student_name"`
	FacultyID     int       `bson:"facultyId" csv:"faculty_id"`
	MerchantID    string    `bson:"merchantId" csv:"merchant_id"`
	IsDeleted     bool      `bson:"isDeleted" csv:"is_deleted"`
	CutOffBy      struct {
		RoleID int    `bson:"roleId" csv:"role_id"`
		Email  string `bson:"email" csv:"email"`
	} `bson:"cutOffBy"`
}

type PostgresData struct {
	StudentID           int     `csv:"studentid"`
	StudentCode         *string `csv:"studentcode"`
	PrefixName          *string `csv:"prefixname"`
	StudentName         *string `csv:"studentname"`
	StudentSurname      *string `csv:"studentsurname"`
	PrefixNameEng       *string `csv:"prefixnameeng"`
	StudentNameEng      *string `csv:"studentnameeng"`
	StudentSurnameEng   *string `csv:"studentsurnameeng"`
	StudentEmailSu      *string `csv:"studentemailsu"`
	AdmitAcadYear       int     `csv:"admitacadyear"`
	AdmitSemester       int     `csv:"admitsemester"`
	FacultyID           int     `csv:"facultyid"`
	FacultyName         *string `csv:"facultyname"`
	FacultyNameEng      *string `csv:"facultynameeng"`
	CampusID            int     `csv:"campusid"`
	CampusName          string  `csv:"campusname"`
	CampusNameEng       string  `csv:"campusnameeng"`
	LevelID             int     `csv:"levelid"`
	LevelName           *string `csv:"levelname"`
	LevelNameEng        *string `csv:"levelnameeng"`
	ScheduleGroupID     int     `csv:"schedulegroupid"`
	ProgramID           int     `csv:"programid"`
	ProgramName         *string `csv:"programname"`
	ProgramNameEng      *string `csv:"programnameeng"`
	StudentStatus       int     `csv:"studentstatus"`
	StudentStatusDesc   string  `csv:"studentstatusdesc"`
	StudentYear         int     `csv:"studentyear"`
	DepartmentID        int     `csv:"departmentid"`
	DepartmentName      *string `csv:"departmentname"`
	DepartmentNameEng   *string `csv:"departmentnameeng"`
	MinorProgramID      *int    `csv:"minorprogramid"`
	MinorProgramName    *string `csv:"minorprogramname"`
	MinorProgramNameEng *string `csv:"minorprogramnameeng"`
	FinanceStatus       string  `csv:"financestatus"`
	LastUpdateTime      string  `csv:"lastupdatetime"`
}

type MergedData struct {
	CollectedDate string `csv:"เก็บวันที่"`
	UsedDate      string `csv:"ใช้วันที่"`
	Status        string `csv:"สถานะ"`
	StudentCode   string `csv:"รหัสนักศึกษา"`
	PrefixName    string `csv:"คำนำหน้า"`
	StudentNameTh string `csv:"ชื่อ-สกุล"`
	StudentName   string `csv:"Student name"`
	FacultyName   string `csv:"คณะ"`
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	credential := options.Credential{
		AuthMechanism: "SCRAM-SHA-1",
		AuthSource:    "db_name",
		Username:      "db_user",
		Password:      "db_password",
	}

	clientOptions := options.Client().ApplyURI("mongodb://localhost:27017").SetAuth(credential)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		log.Fatal("Failed to connect to MongoDB: ", err)
	}

	if err := client.Ping(ctx, readpref.Primary()); err != nil {
		log.Fatal("Failed to ping to MongoDB: ", err)
	}

	fmt.Println("Connect to mongoDB successfully!")

	mongoCollection := client.Database("newsdb").Collection("subVoucher")

	// ดึงข้อมูลจาก MongoDB
	// สร้าง ObjectId
	voucherID, err := primitive.ObjectIDFromHex("668aca9231c1138523291f14")
	if err != nil {
		log.Fatalf("Error converting voucherId to ObjectId: %v", err)
	}

	// ดึงข้อมูลจาก MongoDB โดยใช้ voucherId
	filter := bson.M{"voucherId": voucherID}
	cursor, err := mongoCollection.Find(ctx, filter)
	if err != nil {
		log.Fatal(err)
	}
	defer cursor.Close(ctx)

	var mongoData []MongoData
	if err = cursor.All(ctx, &mongoData); err != nil {
		log.Fatal(err)
	}

	// ตั้งค่า PostgreSQL
	pgConnStr := "user=db_user password=db_password dbname=db_name host=localhost port=5432 sslmode=disable"
	pgDB, err := sql.Open("postgres", pgConnStr)
	if err != nil {
		log.Fatal(err)
	}
	defer pgDB.Close()

	// ทดสอบการเชื่อมต่อ
	err = pgDB.Ping()
	if err != nil {
		log.Fatal("Cannot connect to database:", err)
	}

	fmt.Println("Connected to PostgreSQL successfully!")

	// ดึงข้อมูลจาก PostgreSQL
	rows, err := pgDB.Query("SELECT * FROM app_student")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var pgData []PostgresData
	for rows.Next() {
		var pgItem PostgresData
		err := rows.Scan(
			&pgItem.StudentID,
			&pgItem.StudentCode,
			&pgItem.PrefixName,
			&pgItem.StudentName,
			&pgItem.StudentSurname,
			&pgItem.PrefixNameEng,
			&pgItem.StudentNameEng,
			&pgItem.StudentSurnameEng,
			&pgItem.StudentEmailSu,
			&pgItem.AdmitAcadYear,
			&pgItem.AdmitSemester,
			&pgItem.FacultyID,
			&pgItem.FacultyName,
			&pgItem.FacultyNameEng,
			&pgItem.CampusID,
			&pgItem.CampusName,
			&pgItem.CampusNameEng,
			&pgItem.LevelID,
			&pgItem.LevelName,
			&pgItem.LevelNameEng,
			&pgItem.ScheduleGroupID,
			&pgItem.ProgramID,
			&pgItem.ProgramName,
			&pgItem.ProgramNameEng,
			&pgItem.StudentStatus,
			&pgItem.StudentStatusDesc,
			&pgItem.StudentYear,
			&pgItem.DepartmentID,
			&pgItem.DepartmentName,
			&pgItem.DepartmentNameEng,
			&pgItem.MinorProgramID,
			&pgItem.MinorProgramName,
			&pgItem.MinorProgramNameEng,
			&pgItem.FinanceStatus,
			&pgItem.LastUpdateTime,
		)
		if err != nil {
			log.Fatal(err)
		}

		pgData = append(pgData, pgItem)
	}

	// สร้างแผนที่สำหรับ pgData
	pgMap := make(map[int]PostgresData)
	for _, pgItem := range pgData {
		pgMap[pgItem.StudentID] = pgItem
	}

	// TimeZone +7 (Asia/Bangkok)
	loc, err := time.LoadLocation("Asia/Bangkok")
	if err != nil {
		log.Fatal(err)
	}

	// ทำการ join ข้อมูล
	var mergedData []MergedData
	for _, mongoItem := range mongoData {
		mergeDataItem := MergedData{
			CollectedDate: mongoItem.CollectedDate.In(loc).Format("02/01/2006 15:04:05"),
			UsedDate:      mongoItem.UsedDate.In(loc).Format("02/01/2006 15:04:05"),
			Status:        mongoItem.Status,
			StudentName:   mongoItem.StudentName,
		}
		if pgItem, exists := pgMap[mongoItem.StudentID]; exists {
			mergeDataItem.StudentCode = *pgItem.StudentCode
			mergeDataItem.PrefixName = *pgItem.PrefixName
			mergeDataItem.StudentNameTh = *pgItem.StudentName + " " + *pgItem.StudentSurname
			mergeDataItem.FacultyName = *pgItem.FacultyName
		}
		mergedData = append(mergedData, mergeDataItem)
	}

	// บันทึกข้อมูลลงในไฟล์ CSV
	csvFile, err := os.Create("merged_data.csv")
	if err != nil {
		log.Fatal(err)
	}
	defer csvFile.Close()

	csvWriter := csv.NewWriter(csvFile)
	defer csvWriter.Flush()

	// เขียนข้อมูลในรูปแบบ CSV
	err = gocsv.MarshalFile(&mergedData, csvFile)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("ข้อมูลถูกบันทึกลงในไฟล์ merged_data.csv เรียบร้อยแล้ว")
}
