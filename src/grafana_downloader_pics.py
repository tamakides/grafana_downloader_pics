import requests
import os
import json
from datetime import datetime, date
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse


def pool_prepare(cfg):
    host = cfg['cfg']['host']
    path_dir = cfg['cfg']['path_dir']
    token = cfg['cfg']['token']
    pool_url = []
    from_time = int(datetime.strptime(cfg['cfg']['from'], "%d/%m/%y %H:%M:%S").timestamp()) * 1000
    to_time = int(datetime.strptime(cfg['cfg']['to'], "%d/%m/%y %H:%M:%S").timestamp()) * 1000

    for item in cfg['cfg']['items_pics']:
        if not os.path.exists(os.path.join(path_dir, item['dash'])):
            os.mkdir(os.path.join(path_dir, item['dash']))
        path_pics = os.path.join(path_dir, item['dash'])
        url = host + item['dash_key'] + '/' + item['dash']
        for panelId in item['panelIds'].split(','):
            item['panelId'] = panelId
            item['from'] = from_time
            item['to'] = to_time
            item['path_pics'] = path_pics
            item['url'] = url
            item['token'] = token
            pool_url.append(item.copy())
    return pool_url


def downloader(pool_url):
    r = requests.get(pool_url['url'], stream=True,
                     headers={
                         'Accept': 'application / json',
                         'Authorization': pool_url['token']
                     },
                     params={
                         'from': pool_url['from'],
                         'to': pool_url['to'],
                         'panelId': pool_url['panelId'],
                         'orgId': pool_url['orgId'],
                         'width': pool_url['width'],
                         'height': pool_url['height'],
                         'tz': pool_url['tz']
                     })
    # print('%s panelId = %s' % (r,pool_url['panelId']))
    filename = str(pool_url['panelId']) + '.png'
    with open(os.path.join(pool_url['path_pics'], filename), 'bw') as file:
        for chunk in r.iter_content(4096):
            file.write(chunk)
    return '%s panelId = %s' % (r, pool_url['panelId'])


def parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', dest='config', default='config.json', help='file config, default config.json')
    parser.add_argument('--from_time', dest='from_time', default=None, help='example format "26/05/20 '
                                                                            '09:22:18"')
    parser.add_argument('--to_time', dest='to_time', default=None, help='example format "26/05/20 09:22:18"')
    return parser.parse_args()


def main():
    t0 = time.time()

    args = parse()

    cfg = json.load(open(args.config, "r"))

    if args.from_time and args.to_time is not None:
        cfg['cfg']['from'] = args.from_time
        cfg['cfg']['to'] = args.to_time

    pool_url = pool_prepare(cfg)

    with ThreadPoolExecutor(max_workers=cfg['cfg']['workers']) as pool:
        result = [pool.submit(downloader, i) for i in pool_url]
        for future in as_completed(result):
            print(future.result())

    print('Duration = %s sec' % (time.time() - t0))


if __name__ == '__main__':
    main()
