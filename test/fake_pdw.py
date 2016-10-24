#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tornado.ioloop
import tornado.web


class MainHandler(tornado.web.RequestHandler):
    def put(self):
        print('-' * 80)
        print(self.request)
        print('body:' + self.request.body)


def make_app():
    return tornado.web.Application([
        (r"/vardefs", MainHandler),
    ])

if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()

