/*
 * Copyright 2018-present Open Networking Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import { Node } from './node';
import * as d3 from 'd3';

export enum LinkType {
    UiRegionLink,
    UiDeviceLink,
    UiEdgeLink
}

/**
 * model of the topo2CurrentRegion region rollup from Region below
 *
 */
export interface RegionRollup {
    id: string;
    epA: string;
    epB: string;
    portA: string;
    portB: string;
    type: LinkType;
}

/**
 * Implementing SimulationLinkDatum interface into our custom Link class
 */
export class Link implements d3.SimulationLinkDatum<Node> {
    // Optional - defining optional implementation properties - required for relevant typing assistance
    index?: number;

    // Must - defining enforced implementation properties
    source: Node;
    target: Node;

    constructor(source, target) {
        this.source = source;
        this.target = target;
    }
}

/**
 * model of the topo2CurrentRegion region link from Region below
 */
export class RegionLink extends Link {
    id: string; // The id of the link in the format epA/portA~epB/portB
    epA: string; // The name of the device or host at one end
    epB: string; // The name of the device or host at the other end
    portA: string; // The number of the port at one end
    portB: string; // The number of the port at the other end
    rollup: RegionRollup[]; // Links in sub regions represented by this one link
    type: LinkType;

    constructor(type: LinkType, nodeA: Node, nodeB: Node) {
        super(nodeA, nodeB);
    }
}
