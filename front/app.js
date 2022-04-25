const { request, response } = require('express')
const sqlite3 = require('sqlite3').verbose();

const express = require('express'),
app = express()

app.use('/api/',require('./routes/hello'))

app.use('/api/ciudades',require('./routes/hello'))

let database = new sqlite3.Database('../BDD.db', (err) => {
    if (err) {
      console.error(err.message);
    }
    console.log('Connected to the my database.');
  });

const PORT = 3001

app.listen(PORT,()=>{
    console.log('Listening on Port:',PORT)
})