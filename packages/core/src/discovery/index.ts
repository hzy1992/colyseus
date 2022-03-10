
import { Presence } from '../presence/Presence';

const NODES_SET = 'colyseus:nodes';
const DISCOVERY_CHANNEL = 'colyseus:nodes:discovery';

export interface Node {
  port: number;
  processId: string;
  namespace: string;
}
const os = require('os');
///////////////////获取本机ip///////////////////////
function getIPAdress() {
    var interfaces = os.networkInterfaces();
    console.log('interfaces',interfaces);
    for (var devName in interfaces) {
        var iface = interfaces[devName];
        for (var i = 0; i < iface.length; i++) {
            var alias = iface[i];
            if (alias.family === 'IPv4' && alias.address !== '127.0.0.1' && !alias.internal) {
                return alias.address;
            }
        }
    }
}
const myHost = getIPAdress();


async function getNodeAddress(node: Node) {
  const host = process.env.SELF_HOSTNAME || myHost;
  const port = process.env.SELF_PORT || node.port;
  return `${node.processId}/${host}:${port}`;
}

export async function registerNode(presence: Presence, node: Node) {
  const nodeAddress = await getNodeAddress(node);
  await presence.sadd((node.namespace || "")+NODES_SET, nodeAddress);
  await presence.publish((node.namespace || "")+DISCOVERY_CHANNEL, `add,${nodeAddress}`);
}

export async function unregisterNode(presence: Presence, node: Node) {
  const nodeAddress = await getNodeAddress(node);
  await presence.srem((node.namespace || "")+NODES_SET, nodeAddress);
  await presence.publish((node.namespace || "")+DISCOVERY_CHANNEL, `remove,${nodeAddress}`);
}
