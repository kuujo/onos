#!/usr/bin/env python

"""
"""
from mininet.topo import Topo

class AttMplsTopo( Topo ):
    "Internet Topology Zoo Specimen."

    def addSwitch( self, name, **opts ):
        kwargs = { 'protocols' : 'OpenFlow13' }
        kwargs.update( opts )
        return super(AttMplsTopo, self).addSwitch( name, **kwargs )

    def __init__( self ):
        "Create a topology."

        # Initialize Topology
        Topo.__init__( self )

        # add nodes, switches first...
        s1 = self.addSwitch( 's1' )
        s2 = self.addSwitch( 's2' )
        s3 = self.addSwitch( 's3' )
        s4 = self.addSwitch( 's4' )
        s5 = self.addSwitch( 's5' )
        s6 = self.addSwitch( 's6' )
        s7 = self.addSwitch( 's7' )


        # ... and now hosts
        h1 = self.addHost( 'h1' )
        h2 = self.addHost( 'h2' )
        h3 = self.addHost( 'h3' )
        h4 = self.addHost( 'h4' )

        # add edges between switch and corresponding host
        self.addLink( h1 , s1 )
        self.addLink( h2 , s2 )
        self.addLink( h3 , s6 )
        self.addLink( h4 , s7 )

        self.addLink( s1 , s2 )
        self.addLink( s2 , s3 )
        self.addLink( s3 , s4 )
        self.addLink( s4 , s5 )
        self.addLink( s5 , s6 )
        self.addLink( s6 , s7 )


topos = { 'att': ( lambda: AttMplsTopo() ) }

if __name__ == '__main__':
    from onosnet import run
    run( AttMplsTopo() )
