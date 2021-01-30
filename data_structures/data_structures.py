## LINKED LIST

class Element(object):
    def __init__(self, value):
        self.value = value
        self.next = None

class LinkedList(object):
    def __init__(self, head=None):
        self.head = head

    def append(self, new_element):
        current = self.head
        if self.head:
            while current.next:
                current = current.next
            current.next = new_element
        else:
            self.head = new_element

    def get_position(self, position):
        counter = 1
        current = self.head
        if position < 1:
            return None
        while current and counter <= position:
            if counter == position:
                return current
            current = current.next
            counter += 1
        return None

    def insert(self, new_element, position):
        counter = 1
        current = self.head
        if position > 1:
            while current and counter < position:
                if counter == position - 1:
                    new_element.next = current.next
                    current.next = new_element
                current = current.next
                counter += 1
        elif position == 1:
            new_element.next = self.head
            self.head = new_element

    def delete(self, value):
        current = self.head
        previous = None
        while current.value != value and current.next:
            previous = current
            current = current.next
        if current.value == value:
            if previous:
                previous.next = current.next
            else:
                self.head = current.next



## BINARY TREE

class BinaryTree(object):
    def __init__(self, root):
        self.root = Node(root)

    def insert(self, new_val):
        self.insert_helper(self.root, new_val)

    def insert_helper(self, current, new_val):
        if current.value < new_val:
            if current.right:
                self.insert_helper(current.right, new_val)
            else:
                current.right = Node(new_val)
        else:
            if current.left:
                self.insert_helper(current.left, new_val)
            else:
                current.left = Node(new_val)

    def search(self, find_val):
        #### return true if in the tree, otherwise false
        return self.preorder_search(tree.root, find_val)

    def search_helper(self, current, find_val):
        if current:
            if current.value == find_val:
                return True
            elif current.value < find_val:
                return self.search_helper(current.right, find_val)
            else:
                return self.search_helper(current.left, find_val)
        return False

    def preorder_search(self, start, find_val):
        ## recursive search solution
        if start:
            if start.value == find_val:
                return True
            else:
                return self.preorder_search(start.left, find_val) or self.preorder_search(start.right, find_val)
        return False

    def preorder_print(self, start, traversal):
        ## recurisve print solution
        if start:
            traversal += (str(start.value) + "-")
            traversal = self.preorder_print(start.left, traversal)
            traversal = self.preorder_print(start.right, traversal)
        return traversal


## GRAPH FOR DATA STORAGE
def get_edge_list(self):
    edge_list = []
    for edge_object in self.edges:
        edge = (edge_object.value, edge_object.node_from.value, edge_object.node_to.value)
        edge_list.append(edge)
    return edge_list

def get_adjacency_list(self):
    max_index = self.find_max_index()
    adjacency_list = [None] * (max_index + 1)
    for edge_object in self.edges:
        if adjacency_list[edge_object.node_from.value]:
            adjacency_list[edge_object.node_from.value].append((edge_object.node_to.value, edge_object.value))
        else:
            adjacency_list[edge_object.node_from.value] = [(edge_object.node_to.value, edge_object.value)]
    return adjacency_list

def get_adjacency_matrix(self):
    max_index = self.find_max_index()
    adjacency_matrix = [[0 for i in range(max_index + 1)] for j in range(max_index + 1)]
    for edge_object in self.edges:
        adjacency_matrix[edge_object.node_from.value][edge_object.node_to.value] = edge_object.value
    return adjacency_matrix

def find_max_index(self):
    max_index = -1
    if len(self.nodes):
        for node in self.nodes:
            if node.value > max_index:
                max_index = node.value
    return max_index


class Graph(object):

    def dfs_helper(self, start_node):

        ret_list = [start_node.value]
        start_node.visited = True
        edges_out = [e for e in start_node.edges
                     if e.node_to.value != start_node.value]
        for edge in edges_out:
            if not edge.node_to.visited:
                ret_list.extend(self.dfs_helper(edge.node_to))
        return ret_list

    def bfs(self, start_node_num):


        node = self.find_node(start_node_num)
        self._clear_visited()
        ret_list = []
        # Your code here
        queue = [node]
        node.visited = True
        def enqueue(n, q=queue):
            n.visited = True
            q.append(n)
        def unvisited_outgoing_edge(n, e):
            return ((e.node_from.value == n.value) and
                    (not e.node_to.visited))
        while queue:
            node = queue.pop(0)
            ret_list.append(node.value)
            for e in node.edges:
                if unvisited_outgoing_edge(node, e):
                    enqueue(e.node_to)
        return ret_list

