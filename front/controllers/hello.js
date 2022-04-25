const { response } = require("express");
const { Database } = require("sqlite3");

exports.hello = (req,res) => {
    res.json(
    {
        "hello":["Paco","María"]
    })
};
exports.db = (req,res) => {
    const sqlite3 = require('sqlite3').verbose();

    // open the database
    let db = new sqlite3.Database('../BDD.db');


    /*db.each(`SELECT * FROM mapa`, (err, row) => {
        if (err) {
            console.log(err);
            throw err;
        }
        console.log(`Fila:${row.Fila} Columna:${row.Columna} Atracción: ${row.id_atracciones}`);
    }, () => {
        console.log('query completed')*/
        db.all('SELECT * FROM truemapa', function(err, rows) {
            res.json(rows);
            //console.log(JSON.stringify(rows))
    });
    

    // close the database connection
    db.close();

}
exports.temps = (req,res) => {
    const sqlite3 = require('sqlite3').verbose();

    // open the database
    let db = new sqlite3.Database('../BDD.db');

        db.all('SELECT * FROM temperaturas', function(err, rows) {
            res.json(rows);
            //console.log(JSON.stringify(rows))
    });
    

    // close the database connection
    db.close();

}
