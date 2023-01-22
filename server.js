const express = require('express');
const bodyParser= require('body-parser')
const MongoClient = require('mongodb').MongoClient

const app = express()
app.set('view engine', 'ejs')
app.use(express.static("public"))
app.use(bodyParser.urlencoded({ extended: true }))

const db_url = 'mongodb://localhost:27017'
const db_name = 'bd2'
let db

MongoClient.connect(db_url, { useNewUrlParser: true }, (err, client) => {
  if (err) return console.log(err)
  db = client.db(db_name)
  console.log(`Connected to MongoDB`)
  console.log(`Database: ${db_name}`)
  app.listen(3000, () => console.log('listening on 3000'))
})

/* ---- Begin Logic ---- */

app.get('/', (req, res) => {
  db.collection('wine').aggregate([{
    $match:{
      NumberOfRatings : {$gt: 5000}}},{
    $sample:{
      size: 50}}])
    .toArray().then(results => {
      res.render('wines', { wines: results })
    })
})


app.get('/red', (req, res) => {
  db.collection('wine').find({Tipology : "red"}).toArray().then(results => {
    res.render('wines', { wines: results })
  })
})

app.get('/white', (req, res) => {
  db.collection('wine').find({Tipology : "white"}).toArray().then(results => {
    res.render('wines', { wines: results })
  })
})

app.get('/rose', (req, res) => {
  db.collection('wine').find({Tipology : "rose"}).toArray().then(results => {
    res.render('wines', { wines: results })
  })
})

app.get('/sparkling', (req, res) => {
  db.collection('wine').find({Tipology : "sparkling"}).toArray().then(results => {
    res.render('wines', { wines: results })
  })
})

app.post('/search', (req, res) => {
  var name = req.body.name
  db.collection('wine').find({"Name" : {
    $regex : name, $options : 'i'}}).toArray().then(results => {
      res.render('wines', { wines: results })
    })
})

app.post('/query', (req, res) => {
  var tipology = req.body.tipology
  var field = req.body.field
  var value = parseFloat(req.body.value)
  if (req.body.query_type === 'more') {
    if (tipology === "all"){
      db.collection('wine').find({[field] : {$gte : value}}).sort({[field] : -1}).toArray()
        .then(results => {
          res.render('wines', { wines: results })
        })
    } else {
      db.collection('wine').find({[field] : {$gte : value}, "Tipology" : tipology}).sort({[field] : -1}).toArray()
        .then(results => {
          res.render('wines', { wines: results })
        })
    } 
  }
  if (req.body.query_type === 'less') {
    if (tipology === "all"){
      db.collection('wine').find({[field] : {$lte : value}}).sort({[field] : 1}).toArray()
        .then(results => {
          res.render('wines', { wines: results })
        })
    } else {
      db.collection('wine').find({[field] : {$lte : value}, "Tipology" : tipology}).sort({[field] : 1}).toArray()
        .then(results => {
          res.render('wines', { wines: results })
        })
    } 
  }
  if (req.body.query_type === 'equal') {
    if (tipology === "all"){
      db.collection('wine').find({[field] : value}).sort({"Name" : -1}).toArray()
        .then(results => {
          res.render('wines', { wines: results })
        })
    } else {
      db.collection('wine').find({[field] : value, "Tipology" : tipology}).sort({"Name" : -1}).toArray()
        .then(results => {
          res.render('wines', { wines: results })
        })
    }
  }
})

app.get('/topCountry', (req, res) => {
  db.collection('wine').aggregate([{
    $group: {
      "_id": "$Country",
      "numWines": { "$sum": 1 },
      "numRatings": { "$sum": "$NumberOfRatings" },
      "wines" : { $push: "$_id"}}},{
    $sort: { "numRatings": -1 } },]).toArray().then(results => {
      db.collection("countriesPopularity").insert(results)
      res.render('countries', { countries: results })
    })
})

app.get('/topOrigins', (req, res) => {
  db.collection('wine').aggregate([{
    $group: {
      "_id": "$Region",
      "numWines": { "$sum": 1 },
      "numRatings": { "$sum": "$NumberOfRatings" },
      "country" : { $last: '$Country' },
      "wines" : { $push: "$_id"}}},{
    $sort: { "numRatings": -1 } },]).toArray().then(results => {
      db.collection("originsPopularity").insert(results)
      res.render('origins', { origins: results })
    })
})

app.get('/topWines', (req, res) => {
  db.collection('wine').find().sort({NumberOfRatings:-1}).limit(1).toArray().then(mostPopular => {
    db.collection('wine').find().sort({Price:-1}).limit(1).toArray().then(mostExpensive => {
      db.collection('wine').find().sort({Price:1}).limit(1).toArray().then(cheapest => {
        db.collection('wine').aggregate([
          {$project: { _id:0, Name:1, NumberOfRatings:1, Price:1, Tipology:1,
                       total: {'$multiply': [ "$Rating", "$NumberOfRatings", {'$divide': [1, "$Price"]}]}}},
          { $sort: {total: -1}},
          { $limit: 1}]).toArray().then(best => {
            res.render('hallOfFame', { best:best[0],
              cheapest:cheapest[0],
              mostExpensive:mostExpensive[0],
              mostPopular:mostPopular[0]})
          })
      })
    })
  })
})


var mapFunction = () => {
  emit(this.Country, this.NumberOfRatings);
};

var reduceFunction = (country, ratings) => {
  return Array.sum(ratings);
};

app.get('/topCountry', (req, res) => {
  db.collection('wine').mapReduce(mapFunction, reduceFunction,{out: "countryPopularity"})
  db.collection('countryPopularity').find().sort( { NumberOfRatings : 1 } ).toArray()
    .then(results => {
      res.render('wines', { countries: results })
    })
})



