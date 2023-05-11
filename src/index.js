import {connect, close} from './connection.js';

const db = await connect();
const usersCollection = db.collection("users");
const articlesCollection = db.collection("articles");
const studentsCollection = db.collection("students");

const run = async () => {
    try {
        // await getUsersExample();
        await task1();
        await task2();
        await task3();
        await task4();
        await task5();
        await task6();
        await task7();
        await task8();
        await task9();
        await task10();
        await task11();
        await task12();

        await close();
    } catch (err) {
        console.log('Error: ', err)
    }
}
run();

// #### Users
// - Get users example
// async function getUsersExample () {
//   try {
//     const [allUsers, firstUser] = await Promise.all([
//       usersCollection.find().toArray(),
//       usersCollection.findOne(),
//     ])
//
//     console.log('allUsers', allUsers);
//     console.log('firstUser', firstUser);
//   } catch (err) {
//     console.error('getUsersExample', err);
//   }
// }

// - Get all users, sort them by age (ascending), and return only 5 records with firstName, lastName, and age fields.
async function task1() {
    try {
        const users = await usersCollection
            .find()
            .project(
                {
                    firstName: 1,
                    lastName: 1,
                    age: 1,
                    _id: 0
                }
            )
            .sort('age', 'asc')
            .limit(5)
            .toArray();
        console.log('task 1 completed')
        // console.log('task 1: ', users)
    } catch (err) {
        console.error('task1', err)
    }
}

// - Add new field 'skills: []" for all users where age >= 25 && age < 30 or tags includes 'Engineering'
async function task2() {
    try {
        await usersCollection
            .updateMany(
                {
                    $or: [
                        {
                            $and: [
                                {age: {$gte: 25}},
                                {age: {$lt: 30}}
                            ]
                        },
                        {
                            tags: 'Engineering'
                        }
                    ],

                },
                {$set: {skills: []}}
            );
        console.log('task 2 completed')
    } catch (err) {
        console.error('task2', err)
    }
}

// - Update the first document and return the updated document in one operation (add 'js' and 'git' to the 'skills' array)
//   Filter: the document should contain the 'skills' field
async function task3() {
    try {
        await usersCollection
            .findOneAndUpdate(
                {skills: {$exists: true}},
                {$push: {skills: {$each: ['js', 'git']}}},
                {returnDocument: 'after'}
            );

        console.log('task 3 completed')
    } catch (err) {
        console.error('task3', err)
    }
}

// - REPLACE the first document where the 'email' field starts with 'john' and the 'address state' is equal to 'CA'
//   Set firstName: "Jason", lastName: "Wood", tags: ['a', 'b', 'c'], department: 'Support'
async function task4() {
    try {
        await usersCollection
            .replaceOne(
                {
                    email: {$regex: /^john/},
                    'address.state': 'CA'
                },
                {
                    firstName: 'Jason',
                    lastName: 'Wood',
                    tags: ['a', 'b', 'c'],
                    department: 'Support'
                }
            );
        console.log('task 4 completed');
    } catch (err) {
        console.log('task4', err);
    }
}

// - Pull tag 'c' from the first document where firstName: "Jason", lastName: "Wood"
async function task5() {
    try {
        await usersCollection
            .updateOne(
                {firstName: 'Jason', lastName: 'Wood'},
                {$pull: {tags: 'c'}}
            );
        console.log('task 5 completed');
    } catch (err) {
        console.log('task5', err);
    }
}

// - Push tag 'b' to the first document where firstName: "Jason", lastName: "Wood"
//   ONLY if the 'b' value does not exist in the 'tags'
async function task6() {
    try {
        await usersCollection
            .updateOne(
                {firstName: 'Jason', lastName: 'Wood'},
                {$addToSet: {tags: 'b'}}
            );
        console.log('task 6 completed');
    } catch (err) {
        console.log('task6', err);
    }
}

// - Delete all users by department (Support)
async function task7() {
    try {
        await usersCollection.deleteMany({department: 'Support'});
        console.log('task 7 completed')
    } catch (err) {
        console.log('task7', err);
    }
}

// #### Articles
// - Create new collection 'articles'. Using bulk write:
//   Create one article per each type (a, b, c)
//   Find articles with type a, and update tag list with next value ['tag1-a', 'tag2-a', 'tag3']
//   Add tags ['tag2', 'tag3', 'super'] to articles except articles with type 'a'
//   Pull ['tag2', 'tag1-a'] from all articles
async function task8() {
    try {
        await articlesCollection.bulkWrite([
            {deleteMany: {filter: {}}}, // prevent re-insertion
            {insertOne: {name: 'Mongodb - introduction (a)', description: 'Mongodb - text', type: 'a', tags: []}},
            {insertOne: {name: 'Mongodb - introduction (b)', description: 'Mongodb - text', type: 'b', tags: []}},
            {insertOne: {name: 'Mongodb - introduction (c)', description: 'Mongodb - text', type: 'c', tags: []}},
            {
                updateMany: {
                    filter: {type: 'a'},
                    update: {$set: {tags: ['tag1-a', 'tag2-a', 'tag3']}}
                }
            },
            {
                updateMany: {
                    filter: {type: {$ne: 'a'}},
                    update: {$push: {tags: {$each: ['tag2', 'tag3', 'super']}}}
                }
            },
            {
                updateMany: {
                    filter: {},
                    update: {$pullAll: {tags: ['tag2', 'tag1-a']}}
                }
            }
        ]);
        console.log('task 8 completed');
    } catch (err) {
        console.error('task8', err);
    }
}

// - Find all articles that contains tags 'super' or 'tag2-a'
async function task9() {
    try {
        const articles = await articlesCollection.find({tags: {$elemMatch: {$in: ['super', 'tag2-a']}}}).toArray();
        console.log('task 9 completed')
        // console.log(articles)
    } catch (err) {
        console.log('task9', err);
    }
}

// #### Students Statistic (Aggregations)
// - Find the student who have the worst score for homework, the result should be [ { name: <name>, worst_homework_score: <score> } ]
async function task10() {
    try {
        const worstHomeworkStudent = await studentsCollection
            .aggregate([
                {
                    $unwind: "$scores"
                },
                {
                    $match: {"scores.type": "homework"}
                },
                {
                    $project: {_id: 0, name: 1, worst_homework_score: {$min: "$scores.score"}}
                },
                {
                    $sort: {worst_homework_score: 1}
                },
                {
                    $limit: 1
                }
            ]).toArray();
        // console.log("worstHomeworkStudent", worstHomeworkStudent);
        console.log("task 10 completed")
    } catch (err) {
        console.log('task10', err);
    }
}

// - Calculate the average score for homework for all students, the result should be [ { avg_score: <number> } ]
async function task11() {
    try {
        const avg_score = await studentsCollection
            .aggregate([
                {
                    $unwind: "$scores"
                },
                {
                    $match: {"scores.type": "homework"}
                },
                {
                    $group: {_id: "name", avg_score: {$avg: "$scores.score"}}
                },
                {
                    $project: {_id: 0, avg_score: 1}
                }
            ]).toArray();
        // console.log('avg_score', avg_score);
        console.log('task 11 completed');
    } catch (err) {
        console.log('task11', err);
    }
}

// - Calculate the average score by all types (homework, exam, quiz) for each student, sort from the largest to the smallest value
async function task12() {
    try {
        const avg_score_all = await studentsCollection
            .aggregate([
                {
                    $unwind: "$scores"
                },
                {
                    $group: {_id: "$name", average_score: {$avg: "$scores.score"}}
                },
                {
                    $project: {_id: 1, average_score: 1}
                },
                {
                    $sort: {average_score: -1}
                }
            ]).toArray();
        // console.log('avg_score_all', avg_score_all);
        console.log('task 12 completed');
    } catch (err) {
        console.log('task12', err);
    }
}
