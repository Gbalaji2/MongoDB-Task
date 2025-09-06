use zen_class;
db.dropDatabase();

// Users
db.users.insertMany([
  { _id: 1, name: "Gowtham", batch: "B1" },
  { _id: 2, name: "Siva", batch: "B1" },
  { _id: 3, name: "Mathesh", batch: "B2" },
  { _id: 4, name: "Ram", batch: "B2" }
]);

// Codekata 
db.codekata.insertMany([
  { user_id: 1, problems_solved: 50 },
  { user_id: 2, problems_solved: 80 },
  { user_id: 3, problems_solved: 20 },
  { user_id: 4, problems_solved: 100 }
]);

// Attendance
db.attendance.insertMany([
  { user_id: 1, date: ISODate("2020-10-16"), status: "present" },
  { user_id: 2, date: ISODate("2020-10-17"), status: "absent" },
  { user_id: 3, date: ISODate("2020-10-18"), status: "present" },
  { user_id: 4, date: ISODate("2020-10-19"), status: "absent" }
]);

// Topics
db.topics.insertMany([
  { _id: 1, title: "JavaScript Basics", date: ISODate("2020-10-05") },
  { _id: 2, title: "React Intro", date: ISODate("2020-10-16") },
  { _id: 3, title: "Node.js", date: ISODate("2020-11-01") }
]);

// Tasks
db.tasks.insertMany([
  { topic_id: 1, title: "JS Functions", user_id: 1, submitted: true, date: ISODate("2020-10-06") },
  { topic_id: 2, title: "React Components", user_id: 2, submitted: false, date: ISODate("2020-10-17") },
  { topic_id: 3, title: "Node APIs", user_id: 3, submitted: true, date: ISODate("2020-11-02") }
]);

// Company Drives
db.company_drives.insertMany([
  { _id: 1, company: "Google", date: ISODate("2020-10-16"), students: [1, 2] },
  { _id: 2, company: "Amazon", date: ISODate("2020-10-28"), students: [2, 3] },
  { _id: 3, company: "Facebook", date: ISODate("2020-11-05"), students: [1, 4] }
]);

// Mentors
db.mentors.insertMany([
  { _id: 1, name: "Arun", mentee_count: 10 },
  { _id: 2, name: "Bala", mentee_count: 20 },
  { _id: 3, name: "Kumar", mentee_count: 30 }
]);

// -----------------------------
// Queries
// -----------------------------

print("\n1. Topics and tasks taught in October:");
printjson(
  db.topics.aggregate([
    { $match: { date: { $gte: ISODate("2020-10-01"), $lte: ISODate("2020-10-31") } } },
    {
      $lookup: {
        from: "tasks",
        localField: "_id",
        foreignField: "topic_id",
        as: "related_tasks"
      }
    }
  ]).toArray()
);

print("\n2. Company drives between 15-Oct-2020 and 31-Oct-2020:");
printjson(
  db.company_drives.find({
    date: { $gte: ISODate("2020-10-15"), $lte: ISODate("2020-10-31") }
  }).toArray()
);

print("\n3. Company drives and students appeared for placement:");
printjson(
  db.company_drives.aggregate([
    {
      $lookup: {
        from: "users",
        localField: "students",
        foreignField: "_id",
        as: "appeared_students"
      }
    }
  ]).toArray()
);

print("\n4. Number of problems solved by each user in Codekata:");
printjson(
  db.users.aggregate([
    {
      $lookup: {
        from: "codekata",
        localField: "_id",
        foreignField: "user_id",
        as: "codekata"
      }
    },
    { $unwind: "$codekata" },
    { $project: { name: 1, problems_solved: "$codekata.problems_solved" } }
  ]).toArray()
);

print("\n5. Mentors with mentees count more than 15:");
printjson(
  db.mentors.find({ mentee_count: { $gt: 15 } }).toArray()
);

print("\n6. Users absent and task not submitted between 15-Oct-2020 and 31-Oct-2020:");
printjson(
  db.attendance.aggregate([
    {
      $match: {
        date: { $gte: ISODate("2020-10-15"), $lte: ISODate("2020-10-31") },
        status: "absent"
      }
    },
    {
      $lookup: {
        from: "tasks",
        let: { userId: "$user_id" },
        pipeline: [
          {
            $match: {
              $expr: {
                $and: [
                  { $eq: ["$user_id", "$$userId"] },
                  { $eq: ["$submitted", false] },
                  { $gte: ["$date", ISODate("2020-10-15")] },
                  { $lte: ["$date", ISODate("2020-10-31")] }
                ]
              }
            }
          }
        ],
        as: "pending_tasks"
      }
    },
    { $match: { "pending_tasks.0": { $exists: true } } },
    {
      $lookup: {
        from: "users",
        localField: "user_id",
        foreignField: "_id",
        as: "user"
      }
    }
  ]).toArray()
);
