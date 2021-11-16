import sys
import etl_flow

def main(argv):
    etl_flow(argv)

if __name__=='__main__':
    main(sys.argv[1:])
