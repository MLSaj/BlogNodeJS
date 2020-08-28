const express = require('express')
const session = require('express-session');
const Article = require('./../models/article')
const router = express.Router()

const kafka_topic =  'example'
const kafka_server= 'localhost:2181'
const kafka = require('kafka-node');
const cassandra = require('cassandra-driver');
const { Client, auth } = require('cassandra-driver');


router.get('/new', (req, res) => {
  res.render('articles/new', { article: new Article() })
  Producer = kafka.Producer
  KeyedMessage = kafka.KeyedMessage
  client = new kafka.KafkaClient()
  producer = new Producer(client)
  km = new KeyedMessage('key', 'message')
  payloads = [
    { topic: 'testTopic', messages: 'NewBlog'}
  ];
  producer.send(payloads, function (err, data) {
    console.log(data);
});


})

router.get('/edit/:id', async (req, res) => {
  const article = await Article.findById(req.params.id)
  res.render('articles/edit', { article: article })
})

router.get('/:slug', async (req, res) => {
  const article = await Article.findOne({ slug: req.params.slug })
  //console.log(req.session.vcount)
  if (article == null) res.redirect('/')
  //res.render('articles/show', { article: article })
  console.log(article.title)
  if(!req.session.session_id){
    console.log("making a new one")
    req.session.session_id = Math.floor(Math.random() * (500 - 100) + 100)
    req.sess
  }
  console.log(req.session.session_id)
  res.render('articles/show', { article: article })
  //req.session.save()
  Producer = kafka.Producer
  KeyedMessage = kafka.KeyedMessage
  client = new kafka.KafkaClient()
  producer = new Producer(client)
  km = new KeyedMessage('key', 'message')
  kafka_message = JSON.stringify({ id: req.session.session_id.toString(), url: article.title })
  payloads = [
    { topic: 'eventTopic', messages: kafka_message}
  ];
  producer.send(payloads, function (err, data) {
    console.log(data);
  });
  //console.log(JSON.stringify({ x: req.session.session_id , y: article.title }));


  

  const clientCass = new cassandra.Client({
    contactPoints: ['127.0.0.1:9042'],
    localDataCenter: 'datacenter1', // here is the change required
    keyspace: 'blog',
    authProvider: new auth.PlainTextAuthProvider('cassandra', 'cassandra')
});

console.log(typeof(req.session.session_id))

const query = 'SELECT * FROM recommendations  WHERE id = ?';


clientCass.execute(query, [req.session.session_id],{ hints : ['int'] })
  .then(result => console.log('User with email %s', result.rows[0].recommendations))
  .catch((message) => {
    console.log('Could not find key')
  });
  
 
  
})

router.post('/', async (req, res, next) => {
  req.article = new Article()
  next()
}, saveArticleAndRedirect('new'))

router.put('/:id', async (req, res, next) => {
  req.article = await Article.findById(req.params.id)
  next()
}, saveArticleAndRedirect('edit'))

router.delete('/:id', async (req, res) => {
  await Article.findByIdAndDelete(req.params.id)
  Producer = kafka.Producer
  KeyedMessage = kafka.KeyedMessage
  client = new kafka.KafkaClient()
  producer = new Producer(client)
  km = new KeyedMessage('key', 'message')
  payloads = [
    { topic: 'testTopic', messages: 'Blog is deleted'}
  ];
  producer.send(payloads, function (err, data) {
    console.log(data);
});
  res.redirect('/')
})

function saveArticleAndRedirect(path) {
  return async (req, res) => {
    let article = req.article
    article.title = req.body.title
    article.description = req.body.description
    article.markdown = req.body.markdown
    try {
      article = await article.save()
      res.redirect(`/articles/${article.slug}`)
    } catch (e) {
      res.render(`articles/${path}`, { article: article })
    }
  }
}



module.exports = router