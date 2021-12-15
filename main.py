import tornado.ioloop
import math
import numpy as np
import pandas as pd
import pymongo
from tornado.web import RequestHandler
from sklearn.cluster import KMeans
import calendar, time

class ReturnHandler(RequestHandler):
    def get(self):
        self.db = pymongo.MongoClient(
            'mongodb+srv://<user>:<pass>@cluster0.krf7z.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
        self.db_collection = self.db['denox']['resultados_PAULO']
        self.set_status(200)
        values = [values for values in self.db_collection.find()]
        self.finish(str(values))


class CalculationHandler(RequestHandler):
    def post(self):
        try:
            self.db = pymongo.MongoClient('mongodb+srv://<user>:<pass>@cluster0.krf7z.mongodb.net/myFirstDatabase?retryWrites=true&w=majority')
            self.db_collection = self.db['denox']['dados_rastreamento']

            payload = self.db_collection.find({'serial': self.get_body_argument("serial"),
                                         'datahora':
                                             {'$gt': str(calendar.timegm(time.strptime(self.get_body_argument("initial_datetime"), '%d/%m/%Y %H:%M:%S'))),
                                              '$lt':  str(calendar.timegm(time.strptime(self.get_body_argument("final_datetime"), '%d/%m/%Y %H:%M:%S')))}
                                         })
            tracking = pd.DataFrame([values for values in payload])
            tracking = tracking.sort_values(by='datahora')
            tracking = tracking.astype({'datahora': 'int', 'velocidade': 'float', 'latitude': 'float', 'longitude': 'float'})
            tracking['prev_lat'] = tracking.shift(1)['latitude']
            tracking['prev_lon'] = tracking.shift(1)['longitude']
            tracking['distance'] = tracking.apply(
                lambda row: 0 if np.isnan(row.prev_lat) and np.isnan(row.prev_lon) else math.sqrt((row.latitude - row.prev_lat)**2 + (row.longitude - row.prev_lon)**2), axis=1
            )

            mov_time = tracking[tracking["situacao_movimento"] == 'true']['datahora'].to_list()
            stop_time = tracking[tracking["situacao_movimento"] == 'false']['datahora'].to_list()

            points = np.array(tracking[["latitude", "longitude"]])

            kmeans = KMeans(n_clusters=len(stop_time))
            kmeans.fit(points)


            return_payload = {
                'distancia_percorrida': float(tracking['distance'].sum()),
                'tempo_em_movimento': 0 if (len(mov_time) == 0) else mov_time[len(mov_time) - 1] - mov_time[0],
                'tempo_parado': 0 if (len(stop_time) == 0) else stop_time[len(stop_time) - 1] - stop_time[0],
                'centroides_paradas': kmeans.cluster_centers_.tolist(),
                'serial': self.get_body_argument("serial")
            }
            self.db['denox']['resultados_PAULO'].insert_one(return_payload)
            self.set_status(200)
            self.finish({'status': 200, 'response': str(return_payload)})
        except:
            self.set_status(400)
            self.finish({'status': 400, 'response': ""})



if __name__ == "__main__":
    app = tornado.web.Application([
        (r"/api/calcula_metricas", CalculationHandler),
        (r"/api/retorna_metricas", ReturnHandler)

    ])
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()