/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"
#include <unordered_set>

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

// helper functions
/**
 * FUNCTION NAME: randomIndex
 * 
 * Description: generate a random set of index to gossip
 */
unordered_set<int> randomIndex(int size, int bound) {
    unordered_set<int> sample;
    default_random_engine generator;
    for (int i = bound - size; i < bound; i++) {
        int t = uniform_int_distribution<>(0, i)(generator);
        if (sample.find(t) == sample.end()) {
            sample.insert(t);
        } else {
            sample.insert(i);
        }
    }
    return sample;
}

/**
 * FUNCTION NAME: getAddress
 * 
 * DESCIPTION: return address from id and port
 */ 
Address getAddress(int id, short port) {
    Address addr;
    memset(&addr, 0, sizeof(Address));
    *(int *)(&addr.addr) = id;
    *(short *)(&addr.addr[4]) = port;
    return addr;
}

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	// int id = *(int*)(&memberNode->addr.addr);
	// int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->timeOutCounter = TREMOVE;
	memberNode->pingCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
        memberNode->nnb = 5;
        memberNode->pingCounter = 10;
        int id = *(int*)(&memberNode->addr.addr);
        int port = *(short*)(&memberNode->addr.addr[4]);
        MemberListEntry entry(id, port, 0, par->getcurrtime());
        memberNode->memberList.push_back(entry);
    }
    else {
        size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        memberNode->heartbeat++;
        // create JOINREQ message: format of data is {struct Address myaddr}
        msg->msgType = JOINREQ;
        memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif
        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, msgsize);
        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
    // free(memberNode->addr);
	memberNode->bFailed = false;
	memberNode->inited = false;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->timeOutCounter = TREMOVE;
	memberNode->pingCounter = -1;
    initMemberListTable(memberNode);
    return 0;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
    Address joinaddr = getJoinAddress();
    Member* member = (Member*) env;
    if (member->bFailed) return false;
    // get message type
    MessageHdr* msg = (MessageHdr*) data;
    MsgTypes msgType = msg->msgType;
    if (msgType == JOINREQ) {
        if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr.addr), sizeof(memberNode->addr.addr))) {
            // get message sent address
            char *addr = (char*) malloc(sizeof(memberNode->addr.addr));
            memcpy(addr, (char*)(msg + 1), sizeof(memberNode->addr.addr));
            // get id and port from address
            int id = *(int*)(addr);
            int port = *(short*)(addr + 4);
            // get heartbeat
            long heartbeat;
            memcpy(&heartbeat, (char*)(msg + 1) + 1 + sizeof(member->addr.addr), sizeof(long));
            // add sender to membership list
            MemberListEntry mle(id, port, heartbeat, par->getcurrtime());
            member->memberList.push_back(mle);
            // send JOINGREP back
            // message structure: type + number of entyr + membership list
            size_t msgsize = sizeof(MessageHdr) + sizeof(int);
            for (MemberListEntry mle : member->memberList) {
                msgsize += sizeof(mle);
            }
            // create message
            msg = (MessageHdr*) malloc(msgsize * sizeof(char));
            msg->msgType = JOINREP;
            int numEntry = member->memberList.size();
            memcpy((char*)(msg + 1), &numEntry, sizeof(int));
            for (int i = 0; i < numEntry; i++) {
                memcpy((char*)(msg + 1) + sizeof(int) + i * sizeof(MemberListEntry), &member->memberList[i], sizeof(MemberListEntry));
            }
            // send message
            Address addrPointer = getAddress(id, port);
#ifdef DEBUGLOG
        log->logNodeAdd(&memberNode->addr, &addrPointer);
#endif
            emulNet->ENsend(&memberNode->addr, &addrPointer, (char*) msg, msgsize);
            free(msg);
            free(addr);
        }
    } else if (msgType == JOINREP) {
        if ( 0 != memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr.addr), sizeof(memberNode->addr.addr))) {
            // this node receive JOINREP from introducer
            if (member->inGroup) return true;
            member->inGroup = true;
            member->heartbeat = 0;
            member->nnb = 5;
            member->pingCounter = TFAIL;
            // extract message
            int numEntry;
            memcpy(&numEntry, (char*)(msg + 1), sizeof(int));
            vector<MemberListEntry> memberList;
            for (int i = 0; i < numEntry; i++) {
                MemberListEntry entry;
                memcpy(&entry, (char*)(msg + 1) + sizeof(int) + i * sizeof(MemberListEntry), sizeof(MemberListEntry));
                memberList.push_back(entry);
#ifdef DEBUGLOG
        Address addr = getAddress(entry.getid(), entry.getport());
        log->logNodeAdd(&memberNode->addr, &addr);
#endif
            }
            member->memberList = memberList;
            for (vector<MemberListEntry>::iterator it = member->memberList.begin(); it != member->memberList.end(); it++) {
                Address addr = member->addr;
                Address addr1 = getAddress(it->id, it->port);
                if (addr == addr1) {
                    it->setheartbeat(it->getheartbeat() + 1);
                    it->settimestamp(par->getcurrtime());
                    break;
                }
            }
        }
    } else if (msgType == GOSSIP) {
        // get gossip message, update membership list
        // gossip message structure is similar to reply message structure
        // extract message
        int numEntry;
        memcpy(&numEntry, (char*)(msg + 1), sizeof(int));
        vector<MemberListEntry> memberList;
        for (int i = 0; i < numEntry; i++) {
            MemberListEntry entry;
            memcpy(&entry, (char*)(msg + 1) + sizeof(int) + i * sizeof(MemberListEntry), sizeof(MemberListEntry));
            memberList.push_back(entry);
        }
        // update membership list
        for (MemberListEntry& mle : memberList) {
            bool exists = false;
            for (MemberListEntry& mle1 : member->memberList) {
                if (mle.getid() == mle1.getid() && mle.getport() == mle1.getport()) {
                    exists = true;
                    break;
                }
            }
            if (!exists) {
#ifdef DEBUGLOG
        Address addr = getAddress(mle.getid(), mle.getport());
        log->logNodeAdd(&memberNode->addr, &addr);
#endif
                member->memberList.push_back(mle);
            }
        }
        // }
        for (MemberListEntry& mle : memberList) {
            for (MemberListEntry& mle1 : member->memberList) {
                if (mle.getid() == mle1.getid() && mle.getport() == mle1.getport()) {
                    if (mle1.getheartbeat() < mle.getheartbeat()) {
                        mle1.setheartbeat(mle.getheartbeat());
                        mle1.settimestamp(par->getcurrtime());
                    }
                    break;
                }
            }
        }
    }
    return true;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    for (auto it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
        Address addr = memberNode->addr;
        Address addr1 = getAddress(it->getid(), it->getport());
        if (addr == addr1) {
            it->settimestamp(par->getcurrtime());
            it->setheartbeat(it->getheartbeat() + 1);
            break;
        }
    }
    // check if any node hasn't responded with a timeout period 
    for (int i = 0; i < (int) memberNode->memberList.size(); i++) {
        if (par->getcurrtime() - memberNode->memberList[i].gettimestamp() > memberNode->timeOutCounter) {
            MemberListEntry node = *(memberNode->memberList.begin() + i);
            Address addr = getAddress(node.id, node.port);
            memberNode->memberList.erase(memberNode->memberList.begin() + i);
            #ifdef DEBUGLOG
                log->logNodeRemove(&memberNode->addr, &addr);
            #endif
        }
    }
    memberNode->pingCounter--;
    // propagate membership list
    if (memberNode->pingCounter == 0) {
        memberNode->pingCounter = TFAIL;
        int numEntry = memberNode->memberList.size();
        size_t memlistSize = 0;
        for (MemberListEntry mle : memberNode->memberList) {
            memlistSize += sizeof(mle);
        }
        // message structure: type + number of entyr + membership list
        size_t msgsize = sizeof(MessageHdr) + sizeof(int) + memlistSize;
        // create message
        MessageHdr* msg = (MessageHdr*) malloc(msgsize * sizeof(char));
        msg->msgType = GOSSIP;
        memcpy((char*)(msg + 1), &numEntry, sizeof(int));
        for (int i = 0; i < numEntry; i++) {
            memcpy((char*)(msg + 1) + sizeof(int) + i * sizeof(MemberListEntry), 
                &memberNode->memberList[i], sizeof(MemberListEntry));
        }
        // send message to random neighbors
        for (MemberListEntry entry : memberNode->memberList) {
            // MemberListEntry entry = memberNode->memberList[i];
            Address addr = getAddress(entry.getid(), entry.getport());
            emulNet->ENsend(&memberNode->addr, &addr, (char*) msg, msgsize);
        }
        free(msg);
    }
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

