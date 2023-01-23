//Installato e richiesto il modulo di mongodb
let mongo = require("mongodb");
//Prelevo la parte del modulo per la gestione del client mongo
let mongoClient = mongo.MongoClient;
let  urlServerMongoDb = "mongodb://localhost:27017/";


let http = require("http");
let url = require("url");

let database = "5AInf_2";

//DEFINISCO IL SERVER
let json;
let server = http.createServer(function(req, res){
    //Avverto il browser che ritorno un oggetto JSON
    res.setHeader('Content-Type', 'application/json');

    //Decodifico la richiesta ed eseguo la query interessata
    let scelta = (url.parse(req.url)).pathname;
    switch(scelta){
        

        default:
            json = {cod:-1, desc:"Nessuna query trovata con quel nome"};
            res.end(JSON.stringify(json));
    }
});

server.listen(8888, "127.0.0.1");
console.log("Il server è in ascolto sulla porta 8888");

function creaConnessione(nomeDb, response, callback){
    console.log(mongoClient);
    let promise = mongoClient.connect(urlServerMongoDb);
    promise.then(function(connessione){
        callback(connessione, connessione.db(nomeDb))
    });
    promise.catch(function(err){
        json = {cod:-1, desc:"Errore nella connessione"};
        response.end(JSON.stringify(json));
    });
}

function find(res, obj, select){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(autore).find(obj).project(select).toArray();
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function limit(res, obj, select, n){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(autore).find(obj).project(select).limit(n).toArray();
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function sort(res, obj, select, orderby){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(autore).find(obj).project(select).sort(orderby).toArray();
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function cont(res, query){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(autore).countDocuments(query);
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function cont2(res, query){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(autore).count(query);
        promise.then(function(ris){
            //console.log(ris);
            obj = { cod:0, desc:"Dati trovati con successo", ris};
            res.end(JSON.stringify(obj));
            conn.close();
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function insertOne(res, obj){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(autore).insertOne(obj); 
        promise.then(function(ris){
            json = { cod:1, desc:"Insert in esecuzione", ris };
            res.end(JSON.stringify(json));
            conn.close();
        });
        promise.catch(function(err){
            obj = { cod:-2, desc:"Errore nell'inserimento"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function insertMany(res, array){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(autore).insertMany(array); 
        promise.then(function(ris){
            json = { cod:1, desc:"Insert in esecuzione", ris };
            res.end(JSON.stringify(json));
            conn.close();
        });
        promise.catch(function(err){
            obj = { cod:-2, desc:"Errore nell'inserimento"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}