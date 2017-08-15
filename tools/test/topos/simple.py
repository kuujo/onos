#!/usr/bin/env python

"""
      [1] ----- [2]
"""
from mininet.topo import Topo

class Simple( Topo ):
    """Simple 2 switch example"""

    def __init__( self ):
        """Create a topology."""

        # Initialize Topology
        Topo.__init__( self )

        # add nodes, switches first...
        S1 = self.addSwitch( 's1' )
        S2 = self.addSwitch( 's2' )

        # ... and now hosts
        S1_host = self.addHost( 'h1' )
        S2_host = self.addHost( 'h2' )

        # add edges between switch and corresponding host
        self.addLink( S1, S1_host )
        self.addLink( S2, S2_host )

        # add edges between switches as diagrammed above
        self.addLink( S1, S2, bw=10, delay='1.0ms')

topos = { 'simple': ( lambda: Simple() ) }

if __name__ == '__main__':
    from onosnet import run
    run( Simple() )
