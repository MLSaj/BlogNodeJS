const express = require('express')
const mongoose = require('mongoose')
const Article = require('./models/article')
const articleRouter = require('./routes/articles')
const methodOverride = require('method-override')
const session = require('express-session');
const app = express()
const cassandra = require('cassandra-driver');


app.use(session({
	secret: 'secret-key',
	resave:false,
	saveUninitialized:false,
}));

mongoose.connect('mongodb://localhost/blog', {
  useNewUrlParser: true, useUnifiedTopology: true, useCreateIndex: true
})

const client = new cassandra.Client({ 
  contactPoints: ['host1', 'host2'],
  localDataCenter: 'datacenter1',
  keyspace: 'ks1'
});

app.set('view engine', 'ejs')
app.use(express.urlencoded({ extended: false }))
app.use(methodOverride('_method'))

app.get('/', async (req, res) => {
  const articles = await Article.find().sort({ createdAt: 'desc' })
  res.render('articles/index', { articles: articles })
})



app.use('/articles', articleRouter)



app.listen(5000)