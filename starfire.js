'use strict';

function Starfire(firestoreDB, collectionPath) {

  let onEvent = null;

  if (!firestoreDB) {
    throw("The Firstore Database (firestoreDB) is missing.");
    return null;
  }

  if (!collectionPath || typeof collectionPath !== 'string') {
    throw("The database path (collectionPath) is missing or invalid.");
    return null;
  }

  let DB = firestoreDB;

  let starfire = {};

  let adapt = {
    "to":(key) => {
      return key.toString().replace(/\//g,':');
    },
    "from":(key) => {
      return key.toString().replace(/\:/g,'/');
    }
  };
  
  starfire.onEvent = (cb) => {
    onEvent = cb;
  };

  let EVENT = (e) => {
    if (onEvent && typeof onEvent === 'function') {
      onEvent(e);
    }
  };

  starfire.put = async (key=null, value=null) => {
    if (key.match(/\:/g)) {
      return Promise.reject({"code":400,"message":"The colon (:) is a reserved character."});
    }

    return await DB.collection(collectionPath).doc(adapt.to(key)).set({"key":adapt.to(key),"value":value}).then(() => {
      let e = {"event":"write", "key":key, "timestamp":Date.now()};
      EVENT(e);
      return e;
    }).catch(err => {
      return {"code":400,"message":err.message||err.toString()||"ERROR!"};
    });
  };

  starfire.get = async (key) => {
    return await DB.collection(collectionPath).doc(adapt.to(key)).get().then(result => {
      let value = null;
      let val = result.data();
      if (val && val.value) {
        value = val.value;
      }
      return {"key":key, "value": value||null};
    }).catch(err => {
      return {"code":400,"message":err.message||err.toString()||"ERROR!"};
    });
  };

  starfire.del = async (keys) => {
    let keyPaths = [];
    if (typeof keys === 'string') {
      keyPaths = [keys];
    } else {
      keyPaths = keys;
    }
    let promises = [];
    keyPaths.forEach(key => {
      promises.push(DB.collection(collectionPath).doc(adapt.to(key)).delete());
    });
    let done = await Promise.all(promises).catch(err=>{
      console.log(err);
    });
    let e = {
      "event": "delete",
      "keys": keyPaths,
      "timestamp": Date.now()
    };
    EVENT(e);
    return e;

  };

  starfire.list = async (query) => {
    let order = "asc";
    if (query.reverse) {
      order = "desc";
    }

    let limit = parseInt(query.limit) || 0;

    let ref = DB.collection(collectionPath);

    if (query.gt) {
      ref = ref.where("key", ">", adapt.to(query.gt));
    }

    if (query.lt) {
      ref = ref.where("key", "<", adapt.to(query.lt));
    }

    ref = ref.orderBy("key", order).limit(limit);

    return await ref.get().then(results => {
      let docs = [];
      results.forEach(doc => {
        let item;
        if (query.values) {
          item = {"key":adapt.from(doc.id)};
          item.value = doc.data().value||null;
        } else {
          item = adapt.from(doc.id);
        }

        docs.push(item);

      });
      return docs;
    }).catch(err => {
      return {"code":400,"message":err.message||err.toString()||"ERROR!"};
    });
  };

  starfire.exportDB = async () => {
    return starfire.list({"values":true});
  };

  starfire.importDB = async (items) => {
    let promises = [];
    items.forEach(item => {
      promises.push(starfire.put(item.key, item.value));
    });
    return Promise.all(promises).then( results => {
      let e = {
        "event": "importDB",
        "keys": items.map(val=>{return val.key;}),
        "timestamp": Date.now()
      };
      EVENT(e);
      return e;
    }).catch(err => {
      return {"code":400,"message":err.message||err.toString()||"ERROR!"};
    });
  };

  starfire.deleteDB = async () => {
    return starfire.list({}).then(results=>{
      return starfire.del(results).then(()=>{
        let e = {"event":"deleteDB", "timestamp": Date.now()};
        EVENT(e);
        return e;
      }).catch(err=>{
        return {"code":400,"message":err.message||err.toString()||"ERROR!"};
      });
    });
  
  };

  return starfire;
}

module.exports = Starfire;
