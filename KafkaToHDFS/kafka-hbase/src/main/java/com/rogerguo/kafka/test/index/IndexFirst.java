package com.rogerguo.kafka.test.index;

import com.rogerguo.kafka.test.pointer.Pointer;
import com.rogerguo.kafka.test.consumer.Input_hdfs;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;

class InternalIndexEntry {

    private long startTime;

    private long endTime;

    private Node childNode;

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public Node getChildNode() {
        return childNode;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setChildNode(Node childNode) {
        this.childNode = childNode;
    }

}

class IndexEntry {
    private long startTime;

    private long endTime;

    private String vid;

    private Pointer pointer;

    public long getStartTime() {
        return startTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public String getVid() {
        return vid;
    }

    public Pointer getPointer() {
        return pointer;
    }

    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public void setVid(String vid) {
        this.vid = vid;
    }

    public void setPointer(Pointer pointer) {
        this.pointer = pointer;
    }
}

class timeRange {
    private long sT;

    private long eT;

    public long geteT() {
        return eT;
    }

    public void seteT(long eT) {
        this.eT = eT;
    }

    public long getsT() {
        return sT;
    }

    public void setsT(long sT) {
        this.sT = sT;
    }
}

class DiskChildNodePointer {
    boolean isLeafNode;
    long offsetStart;
}

class Node {
    public int nodeId;
    boolean isLeafNode;
    private long startPos = -1; // disk
    long minT;
    long maxT;
    InternalNode parentNode;
    int parentInternalIndexEntryOrder = -2;
    IndexFirst indexFirst;

    public void setStartPos(long startPos) {
        this.startPos = startPos;
    }

    public long getStartPos() {
        return startPos;
    }
}

class global {
    public static int nId = 0;
    public static int leafNodeSize = 50;
    public static int rootNodeId = 0;
    public static int internalNodeSize = 50;
    public static int oneIndexEntrySize = 80; // in disk
    public static int oneInternalIndexEntrySize = 45; // in disk
    public static int leafNodeBlockSize = leafNodeSize * oneIndexEntrySize; // in disk
    public static int internalNodeBlockSize = internalNodeSize * oneInternalIndexEntrySize; // in disk
    public static String index_file_name = "/test_onefile/index/index.txt";
    public static Input_hdfs ih = new Input_hdfs();

    public static InternalIndexEntry createInternalIndexEntry(Node childNode) {
        InternalIndexEntry itIE = new InternalIndexEntry();
        itIE.setStartTime(childNode.minT);
        itIE.setEndTime(childNode.maxT);
        itIE.setChildNode(childNode);
        return itIE;
    }

    public static HashMap<Integer, LeafNode> leafMap = new HashMap<>();
    public static HashMap<Integer, InternalNode> internalMap = new HashMap<>();

}

public class IndexFirst {
    private LeafNode node;
    private InternalNode rootNode;
    private LeafNode activeNode;
    private int currentStoredNodeNumber = 0;
    public FSDataInputStream dataFile=null;
    public FSDataInputStream index_file=null;
    public FSDataInputStream rootNodeFile=null;
    
    public void setCurrentStoredNodeNumber(int currentStoredNodeNumber) {
        this.currentStoredNodeNumber = currentStoredNodeNumber;
    }

    public int getCurrentStoredNodeNumber() {
        return this.currentStoredNodeNumber;
    }

    public void printLeafMap() {
        for (HashMap.Entry<Integer, LeafNode> entry : global.leafMap.entrySet()) {
            // //////System.out.println("_______________loop _ leaf_________________");
            printSearch(entry.getValue(), 1);
            // //////System.out.println("_______________loop _ leaf_________________");
        }
    }

    public void printInternalMap() {
        for (HashMap.Entry<Integer, InternalNode> entry : global.internalMap.entrySet()) {
            // //////System.out.println("_______________loop _ internal_________________");
            int idE_num = 0;
            for (InternalIndexEntry idE : entry.getValue().internalIndexEntries) {
                // //////System.out.println("index Entry " + idE_num);
                idE_num++;
                printInternalIndexEntry(idE);
            }
            // //////System.out.println("_______________loop _ internal_________________");
        }
    }

    public IndexFirst(String dataFile_name,String index_file_name,String rootNodeFile_name) throws Exception {
        this.node = new LeafNode();
        node.setIndexFirst(this);
        global.nId++;
        this.node.nodeId = global.nId;
        setActiveNode(node);
        global.ih = new Input_hdfs();
        global.ih.init_hdfs();
        // if (!global.ih.isPathExist(dataFile_name)) {
        //      global.ih.fs.create(new Path(dataFile_name));
        // } else {
        //      global.ih.fs.append(new Path(dataFile_name));
        // }

        // if (!global.ih.isPathExist(index_file_name)) {
        //      global.ih.fs.create(new Path(index_file_name));
        // } else {
        //      global.ih.fs.append(new Path(index_file_name));
        // }

        // if (!global.ih.isPathExist(rootNodeFile_name)) {
        //      global.ih.fs.create(new Path(rootNodeFile_name));
        // } else {
        //      global.ih.fs.append(new Path(rootNodeFile_name));
        // }
        dataFile=global.ih.fs.open(new Path(dataFile_name));
        index_file=global.ih.fs.open(new Path(index_file_name));
        rootNodeFile=global.ih.fs.open(new Path(rootNodeFile_name));

    }

    public LeafNode getActiveNode() {
        return activeNode;
    }

    public InternalNode getRootNode() {
        return rootNode;
    }

    public int getRootNodeID() {
        return rootNode.nodeId;
    }

    public void setActiveNode(LeafNode activeNode) {
        this.activeNode = activeNode;
    }

    public void setRootNode(InternalNode rootNode) {
        this.rootNode = rootNode;
    }

    public IndexEntry convertInputToIndexEntry(String vertex_id, String t1, String t2, long offset1, long offset2) {
        IndexEntry iE = new IndexEntry();
        Pointer p = new Pointer();
        iE.setStartTime(Long.parseLong(t1));
        iE.setEndTime(Long.parseLong(t2));
        iE.setVid(vertex_id);
        p.offsetStart = offset1;
        p.offsetEnd = offset2;
        iE.setPointer(p);
        return iE;
    }

    public void indexTreeGeneration(IndexEntry idE, int nLeafNode) throws Exception {
        getActiveNode().insertLeafNode(idE, nLeafNode);
    }

    public void writeLeafNodeIntoIndexFile(LeafNode node, String indexFile) throws Exception {
        // global.ih.init_hdfs();

        long offset = 0;
        Path index_file_path = new Path(indexFile);
        File i_file = new File(indexFile);
        FSDataOutputStream i_outputStream;
        // //////System.out.println("index file exists? "+global.ih.isPathExist(indexFile));
        if (!global.ih.isPathExist(indexFile)) {
            // //////System.out.println("no");
            i_outputStream = global.ih.fs.create(index_file_path);
        } else {
            // //////System.out.println("yes");
            i_outputStream = global.ih.fs.append(index_file_path);
        }
        offset = global.ih.get_file_length(indexFile);
        node.setStartPos(offset);
        String content = "";
        //我感觉写重复了
        //50是满了吧
        //
        //////System.out.println(node.indexEntries.size());
        for (IndexEntry idE : node.indexEntries) {
            content = "";
            content = Long.toString(idE.getStartTime()) + "," + Long.toString(idE.getEndTime()) + "," + idE.getVid()
                    + "," + Long.toString(idE.getPointer().offsetStart) + ","
                    + Long.toString(idE.getPointer().offsetEnd) + ";";
            // content = String.format("%-60s", content);
            content = String.format("%-" + global.oneIndexEntrySize + "s", content);
            // 13+1+13+1+ 8+1+10+1+10+1=79 80

            i_outputStream.writeBytes(content);
        }
        i_outputStream.close();
        setCurrentStoredNodeNumber(getCurrentStoredNodeNumber() + 1);
    }

    public void writeInMemoryNodeInToDisk(String rootNodeFile_name, String index_file_name) throws Exception {

        InternalNode currentParentNode = this.activeNode.parentNode;
        while (currentParentNode != null) {
            if (currentParentNode.internalIndexEntries.size() < global.internalNodeSize) {
                writeIncompleteInternalNodeIntoIndexFile(currentParentNode, index_file_name);
            }
            currentParentNode = currentParentNode.parentNode;
        }        
        writeLeafNodeIntoIndexFile(this.activeNode, rootNodeFile_name);
        writeInternalNodeIntoIndexFile(this.rootNode, rootNodeFile_name);
    }

    public void writeInternalNodeIntoIndexFile(InternalNode node, String indexFile) throws Exception {
        // global.ih.init_hdfs();
        long offset = 0;

        Path index_file_path = new Path(indexFile);

        FSDataOutputStream i_outputStream;

        if (!global.ih.isPathExist(indexFile)) {
            i_outputStream = global.ih.fs.create(index_file_path);
        } else {
            i_outputStream = global.ih.fs.append(index_file_path);
        }
        offset = global.ih.get_file_length(indexFile);
        node.setStartPos(offset);
        String content = "";
        for (InternalIndexEntry itIE : node.internalIndexEntries) {
            content = "";
            content = Long.toString(itIE.getStartTime()) + "," + Long.toString(itIE.getEndTime()) + ","
                    + Boolean.toString(itIE.getChildNode().isLeafNode) + ","
                    + Long.toString(itIE.getChildNode().getStartPos()) + ";";
            content = String.format("%-" + global.oneInternalIndexEntrySize + "s", content);
            i_outputStream.writeBytes(content);
        }
        i_outputStream.close();
        if (node.parentNode != null)
            node.parentNode.internalIndexEntries.get(node.parentInternalIndexEntryOrder).getChildNode()
                    .setStartPos(offset);
    }

    public void writeIncompleteInternalNodeIntoIndexFile(InternalNode node, String indexFile) throws Exception {
        // global.ih.init_hdfs();
        long offset = 0;

        Path index_file_path = new Path(indexFile);

        FSDataOutputStream i_outputStream;

        if (!global.ih.isPathExist(indexFile)) {
            i_outputStream = global.ih.fs.create(index_file_path);
        } else {
            i_outputStream = global.ih.fs.append(index_file_path);
        }
        offset = global.ih.get_file_length(indexFile);
        node.setStartPos(offset);
        String content = "";
        for (InternalIndexEntry itIE : node.internalIndexEntries) {
            content = "";
            content = Long.toString(itIE.getStartTime()) + "," + Long.toString(itIE.getEndTime()) + ","
                    + Boolean.toString(itIE.getChildNode().isLeafNode) + ","
                    + Long.toString(itIE.getChildNode().getStartPos()) + ";";
            content = String.format("%-" + global.oneInternalIndexEntrySize + "s", content);
            i_outputStream.writeBytes(content);
        }
        content = " ";
        content = String.format("%-"
                + (global.internalNodeSize - node.internalIndexEntries.size()) * global.oneInternalIndexEntrySize + "s",
                content);
        i_outputStream.writeBytes(content);

        i_outputStream.close();
        if (node.parentNode != null)
            node.parentNode.internalIndexEntries.get(node.parentInternalIndexEntryOrder).getChildNode()
                    .setStartPos(offset);
    }

    public void newSearch(long startTime, long endTime, List<Pointer> result, String vid, String indexFile,
            String rootFile) throws Exception {
        searchRootNodeInDisk(startTime, endTime, result, vid, this.rootNodeFile, this.index_file, global.leafNodeBlockSize);
    }

    public void searchRootNodeInDisk(long startTime, long endTime, List<Pointer> result, String vid,
            FSDataInputStream rootDataFile, FSDataInputStream indexFile, long nodeStartPos) throws Exception {
        List<DiskChildNodePointer> dCNPs = new ArrayList<>();
        // long ST = System.currentTimeMillis();
        dCNPs.addAll(searchInternalNodeInDisk(nodeStartPos, nodeStartPos + global.internalNodeBlockSize, rootDataFile,
                startTime, endTime));
        // long ET = System.currentTimeMillis();
        // //////System.out.println("InternalNodeInDisk" + (ET - ST) + "ms");
        for (DiskChildNodePointer dCNP : dCNPs) {  
            //    System.out.println("1");
            searchInDisk(startTime, endTime, result, vid, indexFile, dCNP.isLeafNode, dCNP.offsetStart);
        }
    }

    public void searchInDisk(long startTime, long endTime, List<Pointer> result, String vid, FSDataInputStream indexFile,
            boolean isLeafNode, long nodeStartPos) throws Exception {
        if (isLeafNode) {
            //////System.out.println("4");
            result.addAll(searchLeafNodeInDisk(nodeStartPos, nodeStartPos + global.leafNodeBlockSize, indexFile, vid,
                    startTime, endTime));
        } else {
            //System.out.println("2");
            List<DiskChildNodePointer> dCNPs = new ArrayList<>();
            dCNPs.addAll(searchInternalNodeInDisk(nodeStartPos, nodeStartPos + global.internalNodeBlockSize, indexFile,
                    startTime, endTime));
            for (DiskChildNodePointer dCNP : dCNPs) {
                //System.out.println("3");
                searchInDisk(startTime, endTime, result, vid, indexFile, dCNP.isLeafNode, dCNP.offsetStart);
            }

        }
    }

    public List<Pointer> searchLeafNodeInDisk(long offset1, long offset2, FSDataInputStream indexFile, String vid, long startT,
            long endT) throws Exception {
        // Input_hdfs ih_read = new Input_hdfs();
        // ih_read.init_hdfs();
        List<Pointer> pointerList = new ArrayList<>();

        // FSDataInputStream in = global.ih.fs.open(new Path(indexFile));
        Long length = offset2 - offset1;

        byte[] buffer = new byte[length.intValue()];
        indexFile.read(offset1, buffer, 0, length.intValue());
        String entryList = IOUtils.toString(buffer, "utf-8");
        String[] entries = entryList.split(";");

        // binary search algorithm (search)
        int start = 0, end = entries.length - 2, mid = 0,now_v=0;
        String entry = entries[0];
        int v =Integer.parseInt(vid);
        //////System.out.println("start:"+start);
        //////System.out.println("end:"+end);
        //System.out.println("entry.trim():"+entry.trim());
        //////System.out.println("xxx:"+v);
        // for (int i = mid; i < entries.length - 2; i++) {
        //     entry = entries[i];
        //     String[] datas = entry.split(",");
        //     now_v =  Integer.parseInt(datas[2].trim());
        //     //////System.out.println("xxx:"+now_v);
        // }
        while (start <= end && !(entry.trim().equals(""))) {
            //////System.out.println("$$$$:"+now_v);
            mid = (start + end) / 2;
            entry = entries[mid];

            String[] datas = entry.split(",");
            now_v =  Integer.parseInt(datas[2].trim());
            if  (now_v< v) {
                start = mid+1;
            } else if(now_v >v) {
                end = mid-1;
            }else{
                break;
            }
        }

        if(now_v !=v){
            return pointerList;
        }
        // System.out.println("@@@@@@@@@@@");
        while(mid>0){
            entry = entries[mid];
            String[] data1 = entry.split(",");
            int now_v1 =  Integer.parseInt(data1[2].trim());
            entry = entries[mid-1];
            String[] data2 = entry.split(",");
            int now_v2 =  Integer.parseInt(data2[2].trim());
            if(now_v1==now_v2){
                mid --;
            }else{
                break;
            }
        }
        ////System.out.println("@@@@@@@@@@@");
        ////System.out.println(entries[mid]);
        ////System.out.println(startT);
        ////System.out.println(endT);
        for (int i = mid; i < entries.length - 1; i++) {
            entry = entries[i];
            if (!entry.trim().equals("")) {
                String[] datas = entry.split(",");
                if (!(Long.parseLong(datas[0].trim()) > endT || Long.parseLong(datas[1]) < startT)
                        && vid.equals(datas[2])) {
                           ////System.out.println("@@@@@@@@@@@"); 
                    Pointer pointer = new Pointer();
                    pointer.offsetStart = Long.parseLong(datas[3]);
                    pointer.offsetEnd = Long.parseLong(datas[4]);
                    pointerList.add(pointer);
                }
                if (!vid.equals(datas[2].trim())) 
                    break;
            }
        }
        return pointerList;
    }

    public List<DiskChildNodePointer> searchInternalNodeInDisk(long offset1, long offset2, FSDataInputStream indexFile,
            long startT, long endT) throws Exception {
        // Input_hdfs ih_read = new Input_hdfs();
        // ih_read.init_hdfs();
        
        List<DiskChildNodePointer> pointerList = new ArrayList<>();
        // //////System.out.println(indexFile);
        // FSDataInputStream in = global.ih.fs.open(new Path(indexFile));
        // long ST = System.currentTimeMillis();

        Long length = offset2 - offset1;

        byte[] buffer = new byte[length.intValue()];
        indexFile.read(offset1, buffer, 0, length.intValue());
        // long ET = System.currentTimeMillis();
        // //////System.out.println("InternalNodeInDisk" + (ET - ST) + "ms");
        String entryList = IOUtils.toString(buffer, "utf-8");
        String[] entries = entryList.split(";");
        
        for (String entry : entries) {
            if (!entry.trim().equals("")) {
                String[] datas = entry.split(",");
                // System.out.println(entry);
                //////System.out.println(Long.parseLong(datas[0].trim()));
                //////System.out.println(endT);
                //////System.out.println(Long.parseLong(datas[1]));
                //////System.out.println(startT);
                
                if (!(Long.parseLong(datas[0].trim()) > endT || Long.parseLong(datas[1]) < startT)) {
                    DiskChildNodePointer pointer = new DiskChildNodePointer();
                    pointer.isLeafNode = Boolean.parseBoolean(datas[2]);
                    pointer.offsetStart = Long.parseLong(datas[3]);
                    pointerList.add(pointer);
                }
            }
        }
        //////System.out.println("lll"+pointerList.size());
        return pointerList;
    }

    public static List<String> positionRead(FSDataInputStream file, Pointer pointer, long timeStart, long timeEnd, String input_id)
            throws Exception {
        // long startTime = System.currentTimeMillis();
        List<String> resultList = new ArrayList<>();
        // global.ih.init_hdfs();

        // FSDataInputStream in = global.ih.fs.open(new Path(file));
        // int length=49;
        int length = (int) (pointer.offsetEnd - pointer.offsetStart);
        long step = pointer.offsetStart;
        // start 是指初始位置
        // 这几个值自己根据实际情况设置

        byte[] buffer = new byte[length];

        int num = file.read(step, buffer, 0, length);
        String trajectoryList = IOUtils.toString(buffer, "utf-8");
        String[] trajectorys = trajectoryList.split(";");
        int t_size = trajectorys.length - 1;
        int start = 0, end = trajectorys.length - 1, mid = 0;
        String trajectory = "";
        ////System.out.println(end);
        while (start < end) {
            mid = (start + end) / 2;
            trajectory = trajectorys[mid];
            String[] datas = trajectory.split(",");
            if (Long.parseLong(datas[1].trim()) < timeStart) {
                start = mid+1;
            } else {
                end = mid-1;
            }
        }
        for (int i = start; i < t_size; i++) {
            String[] data = trajectorys[i].split(",");
            long given_time = Long.parseLong(data[1]);
            if (given_time >= timeStart) {
                if (given_time <= timeEnd)
                    resultList.add(trajectorys[i]);
                else
                    break;
            }
        }
        // while (true) {
        // // 1 trajectory/ time
        // int num = in.read(step, buffer, 0, length);
        // // ////////System.out.println(IOUtils.toString(buffer,"utf-8"));
        // String data = IOUtils.toString(buffer,"utf-8");
        // String[] datas = data.split(",");
        // // 7,1631697847176,51.841887,27.090206;
        // //判断buffer 是不是
        // long given_time = Long.parseLong(datas[1]);
        // if (given_time >= timeStart && given_time <= timeEnd) {
        // trajectoryList.add(data);
        // }
        // if (step >= pointer.offsetEnd) {
        // break;
        // }
        // step += length;
        // }
        // long endTime = System.currentTimeMillis();
        // ////////System.out.println("查找data file运行时间：" + (endTime - startTime) + "ms");
        // //输出程序运行时间

        return resultList;
    }

    public void search(long startTime, long endTime, List<Pointer> result, String vid, FSDataInputStream indexFile,
            FSDataInputStream rootFile) throws Exception {

        Queue<Node> nodeQueue = new LinkedList<>();

        if (rootNode != null) {
            if (nodeQueue.offer(rootNode)) {
                // //////System.out.println("Success add root");
            } else {
                // //////System.out.println("add root fail");
            }

            while (!nodeQueue.isEmpty()) {
                Node node = nodeQueue.poll();
                // in disk
                if (node.getStartPos() != -1) {
                    searchInDisk(startTime, endTime, result, vid, indexFile, node.isLeafNode, node.getStartPos());
                }

                // in memory
                if (node.isLeafNode) {
                    LeafNode leafNode = (LeafNode) node;
                    leafNodeSearchToResult(leafNode, startTime, endTime, vid, result);
                } else {
                    InternalNode inNode = (InternalNode) node;
                    for (InternalIndexEntry iniIdE : inNode.internalIndexEntries) {
                        if (!(iniIdE.getStartTime() > endTime || iniIdE.getEndTime() < startTime)) {
                            nodeQueue.offer(iniIdE.getChildNode());
                        }
                    }
                }
            }
        }

        if (!connectedToRootNode(activeNode)) {
            leafNodeSearchToResult(activeNode, startTime, endTime, vid, result);
        }
    }

    public void searchFromRootFile(long startTime, long endTime, List<Pointer> result, String vid, FSDataInputStream indexFile,
            FSDataInputStream rootFile) throws Exception {
        searchInDisk(startTime, endTime, result, vid, indexFile, false, global.leafNodeBlockSize);
        // Input_hdfs ih_readDisk = new Input_hdfs();
        // ih_readDisk.init_hdfs();
        // FSDataInputStream in = global.ih.fs.open(new Path(rootFile));

        byte[] buffer = new byte[global.internalNodeBlockSize];
        rootFile.read(global.leafNodeBlockSize, buffer, 0, global.internalNodeBlockSize);
        String interalEntryList = IOUtils.toString(buffer, "utf-8");
        String[] internalEntries = interalEntryList.split(";");
        for (String internalEntry : internalEntries) {
            String[] datas = internalEntry.split(",");
            if (!(Long.parseLong(datas[0].trim()) > endTime || Long.parseLong(datas[1]) < startTime)) {
                searchInDisk(startTime, endTime, result, vid, indexFile, Boolean.parseBoolean(datas[2]),
                        Long.getLong(datas[3]));
            }
        }
    }

    public boolean connectedToRootNode(LeafNode leafNode) {
        InternalNode currentParentNode = leafNode.parentNode;
        while (currentParentNode != null) {
            if (currentParentNode == rootNode)
                return true;
            currentParentNode = currentParentNode.parentNode;
        }
        return false;
    }

    public void leafNodeSearchToResult(LeafNode leafNode, long startTime, long endTime, String vid,
            List<Pointer> result) {
        for (IndexEntry idE : leafNode.indexEntries) {
            if (!(idE.getStartTime() > endTime || idE.getEndTime() < startTime) && vid.equals(idE.getVid())) {
                result.add(idE.getPointer());
            }
        }
    }

    public void printIDSearch(int nid, int deep) {
        // //////System.out.println("deep = " + deep);
        if (global.leafMap.containsKey(nid)) {
            printSearch(global.leafMap.get(nid), deep);
        } else if (global.internalMap.containsKey(nid)) {
            printSearch(global.internalMap.get(nid), deep);
        } else {
            // //////System.out.println("error");
        }
    }

    public void printSearch(LeafNode n, int deep) {
        int idE_num = 0;
        for (IndexEntry idE : n.indexEntries) {
            // //////System.out.println("index Entry " + idE_num);
            idE_num++;
            printIndexEntry(idE);
        }
    }

    public void printSearch(InternalNode n, int deep) {
        int itIE_num = 0;
        for (InternalIndexEntry itIE : n.internalIndexEntries) {
            // //////System.out.println("internal Index Entry " + itIE_num);
            itIE_num++;
            printInternalIndexEntry(itIE);
            printIDSearch(itIE.getChildNode().nodeId, deep + 1);
        }
    }

    public void printIndexEntry(IndexEntry idE) {
        // //////System.out.println(" startTime = " + idE.getStartTime());
        // //////System.out.println(" endTime = " + idE.getEndTime());
        // //////System.out.println(" vid = " + idE.getVid());
        // //////System.out.println(" offsetStart = " + idE.getPointer().offsetStart);
        // //////System.out.println(" offsetEnd = " + idE.getPointer().offsetEnd);
    }

    public void printInternalIndexEntry(InternalIndexEntry itIE) {
        // //////System.out.println(" startTime = " + itIE.getStartTime());
        // //////System.out.println(" endTime = " + itIE.getEndTime());
    }

}

class LeafNode extends Node {
    // index entry
    List<IndexEntry> indexEntries;
    private IndexFirst indexFirst;

    public void setIndexFirst(IndexFirst indexFirst) {
        this.indexFirst = indexFirst;
    }

    public IndexFirst getIndexFirst() {
        return this.indexFirst;
    }

    public LeafNode() {
        isLeafNode = true;
        indexEntries = new ArrayList<>();
        minT = 0;
        maxT = 0;
    }

    public void insertLeafNode(IndexEntry IndexEntry, int nLeafNode) throws Exception {

        if (indexEntries.size() >= global.leafNodeSize) {

            if (indexFirst.getCurrentStoredNodeNumber() >= nLeafNode) {
                // //////System.out.println("there are n leaf node written into index file");
                return;
            }
            //满了就新建 怀疑 刚好满25个 第26个应该是空的
            //但是25满了 就会新建26个
            //不是bug 空的 自然查不出来东西
            //////System.out.println("build new node");
            LeafNode newLeafNode = new LeafNode();
            newLeafNode.setIndexFirst(indexFirst);
            global.nId++;
            newLeafNode.nodeId = global.nId;

            newLeafNode.indexEntries.add(IndexEntry);

            newLeafNode.minT = IndexEntry.getStartTime();
            newLeafNode.maxT = IndexEntry.getEndTime();

            indexFirst.setActiveNode(newLeafNode);
            InternalIndexEntry iIE = global.createInternalIndexEntry(newLeafNode);
            global.leafMap.put(newLeafNode.nodeId, newLeafNode);

            if (this.parentNode != null) {
                if (this.parentNode.internalIndexEntries.size() < global.internalNodeSize) {
                    newLeafNode.parentNode = this.parentNode;
                }
                this.parentNode.insertInternalNode(iIE);
            } else {
                InternalNode newParentNode = new InternalNode(this.indexFirst);

                global.nId++;
                newParentNode.nodeId = global.nId;

                newLeafNode.parentNode = newParentNode;
                this.parentNode = newParentNode;

                InternalIndexEntry oldIIE = global.createInternalIndexEntry(this);
                this.parentNode.insertInternalNode(oldIIE);
                this.parentNode.insertInternalNode(iIE);
                indexFirst.setRootNode(newParentNode);
            }
            // if(indexEntries.size() >= global.leafNodeSize)
            // indexFirst.writeLeafNodeIntoIndexFile(this,global.index_file_name);
            // //////System.out.println("insert internal node");
        } else {
            // //////System.out.println("insert leaf node");
            indexEntries.add(IndexEntry);

            if (IndexEntry.getStartTime() < minT || minT == 0) {
                minT = IndexEntry.getStartTime();
            }
            if (IndexEntry.getEndTime() > maxT)
                maxT = IndexEntry.getEndTime();
            if (indexEntries.size() >= global.leafNodeSize) {
                Collections.sort(indexEntries, new Comparator<IndexEntry>() {
                    public int compare(IndexEntry I1, IndexEntry I2) {
                        int v1  = Integer.parseInt(I1.getVid());
                        int  v2 = Integer.parseInt(I2.getVid());
                        if(v1>v2){
                            return 1;
                        }else if(v1<v2){
                            return -1;
                        }else{
                            return (int) (I1.getStartTime() - I2.getStartTime());
                        }
                        // return (int) (I1.getStartTime() - I2.getStartTime());
                    }
                });
                indexFirst.writeLeafNodeIntoIndexFile(this, global.index_file_name);
            }

            if (this.parentNode != null) {
                parentNode.updateInternalNode(this);
            }
            InternalNode pNode = this.parentNode;
            while (pNode != null && pNode.internalIndexEntries.size() == global.internalNodeSize
                    && pNode.internalIndexEntries.get(global.internalNodeSize - 1).getChildNode().getStartPos() != -1) {

                indexFirst.writeInternalNodeIntoIndexFile(pNode, global.index_file_name);
                pNode = pNode.parentNode;
            }
        }
        global.leafMap.put(this.nodeId, this);
    }
}

class InternalNode extends Node {
    List<InternalIndexEntry> internalIndexEntries;

    public InternalNode(IndexFirst indexFirst) {
        this.setIndexFirst(indexFirst);
        isLeafNode = false;
        internalIndexEntries = new ArrayList<>();
        minT = 0;
        maxT = 0;
    }

    public void setIndexFirst(IndexFirst indexFirst) {
        this.indexFirst = indexFirst;
    }

    public IndexFirst getIndexFirst() {
        return this.indexFirst;
    }

    public void updateInternalNode(Node n) {
        // //////System.out.println("update()");
        InternalNode thisNodesParentNode = n.parentNode;
        while (thisNodesParentNode != null) {
            // //////System.out.println("while have parent node");

            InternalIndexEntry iIE;
            iIE = thisNodesParentNode.internalIndexEntries.get(thisNodesParentNode.internalIndexEntries.size() - 1);

            if (n.minT < iIE.getStartTime()) {
                iIE.setStartTime(n.minT);
            }
            if (n.maxT > iIE.getEndTime()) {
                iIE.setEndTime(n.maxT);
            }

            if (iIE.getStartTime() < thisNodesParentNode.minT || thisNodesParentNode.minT == 0) {
                // //////System.out.println("if1");
                thisNodesParentNode.minT = iIE.getStartTime();
                global.internalMap.put(thisNodesParentNode.nodeId, thisNodesParentNode);
            }
            if (iIE.getEndTime() > thisNodesParentNode.maxT) {
                // //////System.out.println("if2");
                thisNodesParentNode.maxT = iIE.getEndTime();
                global.internalMap.put(thisNodesParentNode.nodeId, thisNodesParentNode);
            }

            thisNodesParentNode = thisNodesParentNode.parentNode;
        }
    }

    public void insertInternalNode(InternalIndexEntry inidE) throws Exception {

        if (internalIndexEntries.size() >= global.internalNodeSize) {

            InternalNode newInternalNode = new InternalNode(this.indexFirst);

            global.nId++;
            newInternalNode.nodeId = global.nId;
            newInternalNode.internalIndexEntries.add(inidE);

            inidE.getChildNode().parentInternalIndexEntryOrder = newInternalNode.internalIndexEntries.size() - 1;

            newInternalNode.minT = inidE.getStartTime();
            newInternalNode.maxT = inidE.getEndTime();
            inidE.getChildNode().parentNode = newInternalNode;
            InternalIndexEntry iIE = global.createInternalIndexEntry(newInternalNode);
            if (this.parentNode != null) {
                if (this.parentNode.internalIndexEntries.size() < global.internalNodeSize) {
                    newInternalNode.parentNode = this.parentNode;
                }
                this.parentNode.insertInternalNode(iIE);
            } else {
                InternalNode newParentNode = new InternalNode(this.indexFirst);

                global.nId++;
                newParentNode.nodeId = global.nId;

                newInternalNode.parentNode = newParentNode;
                this.parentNode = newParentNode;
                InternalIndexEntry oldIIE = global.createInternalIndexEntry(this);
                newParentNode.insertInternalNode(oldIIE);
                newParentNode.insertInternalNode(iIE);
                indexFirst.setRootNode(newParentNode);
                global.internalMap.put(newParentNode.nodeId, newParentNode);
            }
            global.internalMap.put(newInternalNode.nodeId, newInternalNode);
        } else {
            internalIndexEntries.add(inidE);
            inidE.getChildNode().parentInternalIndexEntryOrder = internalIndexEntries.size() - 1;
            if (inidE.getStartTime() < minT || inidE.getEndTime() > maxT || minT == 0) {
                if (inidE.getStartTime() < minT || minT == 0)
                    minT = inidE.getStartTime();
                if (inidE.getEndTime() > maxT)
                    maxT = inidE.getEndTime();
                if (this.parentNode != null)
                    parentNode.updateInternalNode(this);
            }
        }
        global.internalMap.put(this.nodeId, this);
    }
}