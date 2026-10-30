import argparse
from pathlib import Path

def main():
    p=argparse.ArgumentParser(description='FlowLedger: auditable payment pipelines')
    p.add_argument('--root',type=Path,default=Path('data'))
    sub=p.add_subparsers(dest='command',required=True)
    g=sub.add_parser('generate');g.add_argument('--orders',type=int,default=1000);g.add_argument('--date',default='2026-10-01')
    for name in ['batch','stream']:
        c=sub.add_parser(name);c.add_argument('--max-bad-ratio',type=float,default=.2)
        if name=='stream':c.add_argument('--once',action='store_true')
    e=sub.add_parser('emit');e.add_argument('--count',type=int,default=5);e.add_argument('--interval',type=float,default=1)
    sub.add_parser('report');sub.add_parser('check')
    args=p.parse_args()
    if hasattr(args,'max_bad_ratio') and not 0<=args.max_bad_ratio<=1:p.error('--max-bad-ratio must be between 0 and 1')
    if args.command=='generate':
        from .generate import generate
        print(generate(args.root,args.orders,args.date))
    elif args.command=='emit':
        from .generate import emit
        emit(args.root,args.count,args.interval)
    elif args.command=='batch':
        from .pipeline import batch
        batch(args.root,args.max_bad_ratio)
    elif args.command=='stream':
        from .pipeline import stream
        stream(args.root,args.once,args.max_bad_ratio)
    else:
        from .report import report,check
        {'report':report,'check':check}[args.command](args.root)
if __name__=='__main__':main()
