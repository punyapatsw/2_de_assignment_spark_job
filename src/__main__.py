import sys
from etl_flow import etl_flow

def main(argv):
    print(argv)
    etl_flow.etl_flow(argv[0], argv[1][0:10])

if __name__=='__main__':
    main(sys.argv[1:])
