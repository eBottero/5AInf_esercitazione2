//Installato e richiesto il modulo di mongodb
let mongo = require("mongodb");
//Prelevo la parte del modulo per la gestione del client mongo
let mongoClient = mongo.MongoClient;
let  urlServerMongoDb = "mongodb://localhost:27017/";


let http = require("http");
let url = require("url");

let database = "5AInf_2";

//DEFINISCO IL SERVER
let json, op;
let server = http.createServer(function(req, res){
    //Avverto il browser che ritorno un oggetto JSON
    res.setHeader('Content-Type', 'application/json');

    //Decodifico la richiesta ed eseguo la query interessata
    let scelta = (url.parse(req.url)).pathname;
    switch(scelta){
        case "/i1":
            insertMany(res, "transazioni", 
            [
                {mittente:4, destinatario:3, somma:54.6, data:new Date("2020-08-16")},
                {mittente:3, destinatario:5, somma:20.0, data:new Date("2020-09-18")},
                {mittente:4, destinatario:3, somma:5.60, data:new Date("2020-10-23")},
                {mittente:5, destinatario:2, somma:14.3, data:new Date("2020-12-03")},
                {mittente:2, destinatario:6, somma:12.0, data:new Date("2021-01-14")},
                {mittente:8, destinatario:5, somma:100.0, data:new Date("2021-01-20")},
                {mittente:1, destinatario:3, somma:45.0, data:new Date("2021-01-22")},
                {mittente:8, destinatario:2, somma:34.8, data:new Date("2021-01-22")},
                {mittente:3, destinatario:7, somma:200.0, data:new Date("2021-01-27")}
                ]
            ,{});
            break;

        case "/i2":
            insertMany(res, "utenti", 
            [
                {_id:1, nome:"Carlo", cognome:"Ferrero", residenza:"Fossano", anni:54},
                {_id:2, nome:"Leopoldo", cognome:"Marengo", residenza:"Cuneo", anni:65},
                {_id:3, nome:"Mattia", cognome:"Manzo", residenza:"Bra", anni:22},
                {_id:4, nome:"Rosanna", cognome:"Gelso", residenza:"Savigliano", anni:35},
                {_id:5, nome:"Margherita", cognome:"Pea", residenza:"Cuneo", anni:18},
                {_id:6, nome:"Leone", cognome:"Manzo", residenza:"Fossano", anni:43},
                {_id:7, nome:"Albana", cognome:"Renzi", residenza:"Bra", anni:48},
                {_id:8, nome:"Elisa", cognome:"Basso", residenza:"Savigliano", anni:31}
            ]
            ,{});
            break;
        case "/q1":
            // 1. Indica gli utenti che sono residenti a Fossano
            find(res, "utenti",{residenza:"Fossano"},{});
            break;
        case "/q2":
            // Indica gli utenti il cui nome iniziano con C e L con anni superiori a 50.
            find(res, "utenti",{$or:[{nome:/^C/}, {nome:/^L/}], anni:{$gt: 50}},{});
            break;
        case "/q3":
            // 3. Indica i primi due utenti il cui cognome finisce con o visualizzando però solo nome e cognome.
                        op = [
                            {$match:{cognome:/o$/}}, // Finiscono con la 'o' WHERE
                            {$project:{nome: 1, cognome:1}}, 
                            {$sort:{nome:1}}, // ordine crescente
                            {$limit:2}, // limit         
                        ];
                        aggregate(res, "utenti", op)
                        break;
            break;
        case "/q4":
            // Raggruppa in base alla residenza e calcola la media degli anni degli utenti raggruppati.
            op = [
                {
                  $group: {
                    _id: { residenza: "$residenza" },
                    mediaAnni: { $avg: "$anni" },
                  },
                },
              ];
              aggregate(res, "utenti", op);
            break;
            
        case "/q5":
            // Visualizza tutte le transazioni dell’utente Rosanna Gelso nascondendo le date.
            find(res, "transazioni",{mittente:4},{data:0});
            break;
        case "/q6":
            // Conta il numero di transazioni la cui somma inviata è maggiore di 20 euro.
            cont(res, "transazioni", {somma:{$gt:20}})
            break;
        case "/q7":
            // Calcola il bilancio totale (non di fine anno) di Mattia Manzo
            op = [
                // Group:{indico tutti gli attrivuti che verrano visualizzati/calcolati}
                {$group:{mittente:3, somma:{$sum: "$somma"}}},
                
            ];
            aggregate(res, "utenti", op)
            break;
        case "/q8":
            // Raggruppa i destinatari e somma per ciascuno il denaro ricevuto
            opt = [
                {
                  $group: {
                    _id: { destinatario: "$destinatario" },
                    sumDenaro: { $sum: "$somma" },
                  },
                },
              ];
              aggregate(res, "transazioni", opt);
            break;
        case "/q9":
            // Preleva le transazioni effettuate dopo il 01 gennaio
            find(res, "transazioni",{data:{$gt:new Date("2021-01-01")}},{});
            break;
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

function find2(res, col, obj, select, callback){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).find(obj).project(select).toArray();
        promise.then(function(ris){
            conn.close();
            callback(ris);
        });

        promise.catch(function(error){
            obj = { cod:-2, desc:"Errore nella ricerca"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}

function find(res, col, obj, select){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).find(obj).project(select).toArray();
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

/*
    aggregate -> aggregazione di funzioni di ricerca

    opzioni -> array di oggetti dove ogni oggetto è un 
            filtro che vogliamo applicare alla collezione

*/
function aggregate(res, col, opzioni){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).aggregate(opzioni).toArray();
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

function limit(res, col, obj, select, n){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).find(obj).project(select).limit(n).toArray();
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

function sort(res, col, obj, select, orderby){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).find(obj).project(select).sort(orderby).toArray();
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

function cont(res, col, query){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).countDocuments(query);
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

function cont2(res, col, query){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).count(query);
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

function insertOne(res, col, obj){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).insertOne(obj); 
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

function insertMany(res, col, array){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).insertMany(array); 
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

function remove(res, col, where){
    creaConnessione(database, res, function(conn, db){
        let promise = db.collection(col).deleteMany(where); 
        promise.then(function(ris){
            json = { cod:1, desc:"Remove in esecuzione", ris };
            res.end(JSON.stringify(json));
            conn.close();
        });
        promise.catch(function(err){
            obj = { cod:-2, desc:"Errore nella cancellazione"}
            res.end(JSON.stringify(obj));
            conn.close();
        });
    });
}