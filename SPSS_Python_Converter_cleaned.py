import modeler.api
import time
import sys
import ast
import tokenize
from StringIO import StringIO
from cStringIO import StringIO as cStringIO
import os
import re
import shutil



# Path to put folder containing the code
PATH = "C:/Users/Jason.Conte/Desktop/GeneratedCode/"
# Name of the folder containing the code
model_name = "SPX"






stream = modeler.script.stream()

# Allows handling of long nested function calls
sys.setrecursionlimit(2000)

# Class to find SPSS nodes by ID
class IDFilter(modeler.api.NodeFilter):
    def __init__(this, id):
        this.id = id
    def accept(this, node):
        return node.getID() == this.id

# Class to find all SPSS nodes that have no predecessors
class SourceFilter(modeler.api.NodeFilter):
    def accept(this, node):
        return not node.getProcessorDiagram().predecessors(node)

start_node_list = []
def isRealNode(node):
    '''Returns if node is a real node ie/ the python conversion of the node is non-empty
    ARGS:
        - node - SPSS node to check if real

    Conditions for a node to be non-real:
        - Node type is one of: source_super, process_super, terminal_super, input_connector, output_connector, type, reorder, or table
        - Node ID is explicitly ignored in 'ignore_node_list' parameter of config.json file
        - Node ID is manually specified in start_nodes dict
        - Node is a filter node where nothing is dropped or renamed
        - Node is a filler, derive (multi-field), or sort node where no real columns are specified
        - Node is an empty select node
        - Node is a distinct node with no real columns and 'include record count' is not specified
        - Node is a merge node with only one predecessor and no field renaming or filtering
    '''
    input_cols = set()
    # All of the field names going into the node
    for col in node.getInputDataModel():
        input_cols.add(col.getName())
    # For merge nodes, check if any columns are being filtered or renamed after the merge
    if node.getTypeName() == "merge" and len(node.getProcessorDiagram().predecessors(node)) < 2:
        merge_filter_or_rename = False
        tags = list(node.getFilterTableGroup().tagsInUse())
        for tag in tags:
            filterTable = node.getFilterTableGroup().getFilterTable(tag)
            if not filterTable is None:
                for col_name in filterTable.inputFieldSet:
                    col_name = str(col_name)
                    merge_filter_or_rename |= filterTable.getDropped(col_name) | (col_name != filterTable.getNewName(col_name))
        
    return (
        node.getTypeName() not in ["source_super","process_super","terminal_super", "input_connector", "output_connector", "type", "reorder", "table"]
        and node.getID() not in config["ignore_node_list"]
        and node.getID() not in start_node_list
        and not (
            node.getTypeName() == "filter"
            and not set(node.getKeyedPropertyKeys("include")).intersection(input_cols)
            and not set(node.getKeyedPropertyKeys("new_name")).intersection(input_cols)
        )
        and not (
            node.getTypeName() == "filler"
            and not set(node.getPropertyValue("fields")).intersection(input_cols)
        )
        and not (
            node.getTypeName() == "derive"
            and node.getPropertyValue("mode") == "Multiple"
            and not set(node.getPropertyValue("fields")).intersection(input_cols)
        )
        and not (
            node.getTypeName() == "select"
            and re.search("^\s*$",node.getPropertyValue("condition"))
        )
        and not (
            node.getTypeName() == "sort"
            and not set([key for key, ascending in node.getPropertyValue("keys")]).intersection(input_cols)
        )
        and not(
            node.getTypeName() == "distinct"
            and (
                not set(node.getPropertyValue("grouping_fields")).intersection(input_cols)
                or not input_cols - set(node.getPropertyValue("grouping_fields"))
            )
            and (
                not node.getPropertyValue("mode") == "Composite"
                or (
                    node.getPropertyValue("mode") == "Composite" 
                    and not node.getPropertyValue("inc_record_count")
                )
            )
        )
        and not(
            node.getTypeName() == "merge"
            and len(node.getProcessorDiagram().predecessors(node)) < 2
            and not merge_filter_or_rename
        )
    )

# Class to find all SPSS nodes that have no successors
class LeafFilter(modeler.api.NodeFilter):
    def accept(this, node):
        return not node.getProcessorDiagram().successors(node) and (isRealNode(node) or node.getProcessorDiagram().predecessors(node))

# Make code location directories if they do not exist
if not os.path.exists(PATH + model_name):
    os.makedirs(PATH + model_name)
# Create empty config.json file if it does not exist
if not os.path.exists(PATH + model_name + "/config.json"):
    config = {
        "ignore_node_list": [],
        "sections": [],
        "parquet_output_nodes": [],
        "default_csv_encoding":"",
    }
    text = str(config).replace("'",'"')
    f = open(PATH + model_name + "/config.json", 'w')
    f.writelines(text)
    f.close()

# Reads in config.json
f = open(PATH + model_name + "/config.json", 'r')
config = eval(f.read())
f.close()
# If code is broken up into sections, this is the name of the folder those sections will be put into
stream_folder_name = "python_files"

# ----------------------------------------------------------------------------------------------
# If you want it to start converting further down in the stream (not at the start nodes) you can specify that here in the following form: start_nodes = {"id2UQ1ZDPTL9SS":'a.csv'}
start_nodes = {}
# ----------------------------------------------------------------------------------------------

# If sections are not defined in 'sections' parameter of config.json file, convert the entire stream, to every leaf node
if not config["sections"]:
    leaf_ids = [node.getID() for node in stream.findAll(LeafFilter(), True)]
    config["sections"] = [["main",leaf_ids]]
# If sections are defined in 'sections' parameter of config.json file, create directory to contain the code conversion of these sections
if not os.path.exists(PATH + model_name + "/" + stream_folder_name) and (len(config["sections"]) > 1 or (len(config["sections"]) == 1 and type(config["sections"][0][1][0]) == list)):
    os.makedirs(PATH + model_name + "/" + stream_folder_name)

# Refactoring sections into more verbose form
for i in range(len(config["sections"])):
    run, sections = config["sections"][i]
    if type(sections[0]) != list:
        config["sections"][i][1] = [[run,sections]]
# Fills dictionary mapping section name to the sections end node ids as specified in config.json
export_nodes = {}
for i in range(len(config["sections"])):
    for name,section in config["sections"][i][1]:
        export_nodes[name] = section

def get_stream_obj(stream_name):
    '''Given a stream name, looks through the streams currently open in SPSS modeler to see if any stream matches that name. If one does, returns the stream object of that stream
    ARGS:
        stream_name: str
    '''
    stream_manager = modeler.script.session().getStreamManager()
    for i in range(stream_manager.getProcessorStreamCount()):
        cur_stream = stream_manager.getProcessorStreamAt(i)
        if cur_stream.getName() == stream_name:
            return cur_stream
    raise Exception("Stream \"" + stream_name + "\" is not currently open in SPSS Modeler")

# Whether or not to split the conversion into multiple sections
split_streams = not(len(config["sections"]) == 1 and len(config["sections"][0][1]) <= 1) or model_name == "Test"
# If so, creates a temporary stream with the suffix '_TEMP' to copy each section into before converting. This speeds up conversion considerably
if split_streams:
    taskrunner = modeler.script.session().getTaskRunner()
    try:
        taskrunner.createStream(model_name + "_TEMP", False, True)
    except:
        pass
    temp_stream = get_stream_obj(model_name + "_TEMP")   

template_folder_exists = os.path.exists(PATH + "_TEMPLATE/")

letters=['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o','p','q','r','s','t','u','v','w','x','y','z']
letters = letters + [i+j for i in letters for j in letters]
def printRoman(number):
    '''Converts integer to roman numeral equivalent
    ARGS:
        number: int
    RETURNS: str
    '''
    roman_num = ""
    num = [1, 4, 5, 9, 10, 40, 50, 90,
        100, 400, 500, 900, 1000]
    sym = ["i", "iv", "v", "ix", "x", "xl",
        "l", "xc", "c", "cd", "d", "cm", "m"]
    i = 12
     
    while number:
        div = number // num[i]
        number %= num[i]
 
        while div:
            roman_num += sym[i]
            div -= 1
        i -= 1
    return roman_num

# List of first 700 roman numerals
roman_numerals = [printRoman(i) for i in range(1,700)]

sys.path.append(PATH + model_name + "/")
# Checks if there is a manual code map to look at
try:
    from ManualCodeMap import manual_code_map
except:
    manual_code_map = {}
# Expanding tuples in manual code map to make it easier to work with
manual_code_map_expanded = {}
for key,val in manual_code_map.items():
    if type(key) == tuple:
        for nodeid in key:
            manual_code_map_expanded[nodeid] = val
    else:
        manual_code_map_expanded[key] = val

# ------------------------AST UNPARSE-----------------
# This is not my code. Its the Apr 27, 2007 revision of unparse.py, built to unparse the currently installed version of ast in SPSS
# https://github.com/python/cpython/blob/d8faa3654c2887eaa146dcdb553a9f9793bd2e5a/Demo/parser/unparse.py

def interleave(inter, f, seq):
    """Call f on each item in seq, calling inter() in between.
    """
    seq = iter(seq)
    try:
        f(seq.next())
    except StopIteration:
        pass
    else:
        for x in seq:
            inter()
            f(x)

class Unparser:
    """Methods in this class recursively traverse an AST and
    output source code for the abstract syntax; original formatting
    is disregarged. """

    def __init__(self, tree, file = sys.stdout):
        """Unparser(tree, file=sys.stdout) -> None.
         Print the source for tree to file."""
        self.f = file
        self._indent = 0
        self.dispatch(tree)
        print >>self.f,""
        self.f.flush()

    def fill(self, text = ""):
        "Indent a piece of text, according to the current indentation level"
        self.f.write("\n"+"    "*self._indent + text)

    def write(self, text):
        "Append a piece of text to the current line."
        self.f.write(text)

    def enter(self):
        "Print ':', and increase the indentation."
        self.write(":")
        self._indent += 1

    def leave(self):
        "Decrease the indentation level."
        self._indent -= 1

    def dispatch(self, tree):
        "Dispatcher function, dispatching tree type T to method _T."
        if isinstance(tree, list):
            for t in tree:
                self.dispatch(t)
            return
        meth = getattr(self, "_"+tree.__class__.__name__)
        meth(tree)


    ############### Unparsing methods ######################
    # There should be one method per concrete grammar type #
    # Constructors should be grouped by sum type. Ideally, #
    # this would follow the order in the grammar, but      #
    # currently doesn't.                                   #
    ########################################################

    def _Module(self, tree):
        for stmt in tree.body:
            self.dispatch(stmt)

    # stmt
    def _Expr(self, tree):
        self.fill()
        self.dispatch(tree.value)

    def _Import(self, t):
        self.fill("import ")
        interleave(lambda: self.write(", "), self.dispatch, t.names)

    def _ImportFrom(self, t):
        self.fill("from ")
        self.write(t.module)
        self.write(" import ")
        interleave(lambda: self.write(", "), self.dispatch, t.names)
        # XXX(jpe) what is level for?

    def _Assign(self, t):
        self.fill()
        for target in t.targets:
            self.dispatch(target)
            self.write(" = ")
        self.dispatch(t.value)

    def _AugAssign(self, t):
        self.fill()
        self.dispatch(t.target)
        self.write(" "+self.binop[t.op.__class__.__name__]+"= ")
        self.dispatch(t.value)

    def _Return(self, t):
        self.fill("return")
        if t.value:
            self.write(" ")
            self.dispatch(t.value)

    def _Pass(self, t):
        self.fill("pass")

    def _Break(self, t):
        self.fill("break")

    def _Continue(self, t):
        self.fill("continue")

    def _Delete(self, t):
        self.fill("del ")
        self.dispatch(t.targets)

    def _Assert(self, t):
        self.fill("assert ")
        self.dispatch(t.test)
        if t.msg:
            self.write(", ")
            self.dispatch(t.msg)

    def _Print(self, t):
        self.fill("print ")
        do_comma = False
        if t.dest:
            self.write(">>")
            self.dispatch(t.dest)
            do_comma = True
        for e in t.values:
            if do_comma:self.write(", ")
            else:do_comma=True
            self.dispatch(e)
        if not t.nl:
            self.write(",")

    def _Global(self, t):
        self.fill("global ")
        interleave(lambda: self.write(", "), self.write, t.names)

    def _Yield(self, t):
        self.write("(")
        self.write("yield")
        if t.value:
            self.write(" ")
            self.dispatch(t.value)
        self.write(")")

    def _Raise(self, t):
        self.fill('raise ')
        if t.type:
            self.dispatch(t.type)
        if t.inst:
            self.write(", ")
            self.dispatch(t.inst)
        if t.tback:
            self.write(", ")
            self.dispatch(t.tback)

    def _TryExcept(self, t):
        self.fill("try")
        self.enter()
        self.dispatch(t.body)
        self.leave()

        for ex in t.handlers:
            self.dispatch(ex)
        if t.orelse:
            self.fill("else")
            self.enter()
            self.dispatch(t.orelse)
            self.leave()

    def _TryFinally(self, t):
        self.fill("try")
        self.enter()
        self.dispatch(t.body)
        self.leave()

        self.fill("finally")
        self.enter()
        self.dispatch(t.finalbody)
        self.leave()

    def _excepthandler(self, t):
        self.fill("except")
        if t.type:
            self.write(" ")
            self.dispatch(t.type)
        if t.name:
            self.write(", ")
            self.dispatch(t.name)
        self.enter()
        self.dispatch(t.body)
        self.leave()

    def _ClassDef(self, t):
        self.write("\n")
        self.fill("class "+t.name)
        if t.bases:
            self.write("(")
            for a in t.bases:
                self.dispatch(a)
                self.write(", ")
            self.write(")")
        self.enter()
        self.dispatch(t.body)
        self.leave()

    def _FunctionDef(self, t):
        self.write("\n")
        for deco in t.decorators:
            self.fill("@")
            self.dispatch(deco)
        self.fill("def "+t.name + "(")
        self.dispatch(t.args)
        self.write(")")
        self.enter()
        self.dispatch(t.body)
        self.leave()

    def _For(self, t):
        self.fill("for ")
        self.dispatch(t.target)
        self.write(" in ")
        self.dispatch(t.iter)
        self.enter()
        self.dispatch(t.body)
        self.leave()
        if t.orelse:
            self.fill("else")
            self.enter()
            self.dispatch(t.orelse)
            self.leave

    def _If(self, t):
        self.fill("if ")
        self.dispatch(t.test)
        self.enter()
        # XXX elif?
        self.dispatch(t.body)
        self.leave()
        if t.orelse:
            self.fill("else")
            self.enter()
            self.dispatch(t.orelse)
            self.leave()

    def _While(self, t):
        self.fill("while ")
        self.dispatch(t.test)
        self.enter()
        self.dispatch(t.body)
        self.leave()
        if t.orelse:
            self.fill("else")
            self.enter()
            self.dispatch(t.orelse)
            self.leave

    def _With(self, t):
        self.fill("with ")
        self.dispatch(t.context_expr)
        if t.optional_vars:
            self.write(" as ")
            self.dispatch(t.optional_vars)
        self.enter()
        self.dispatch(t.body)
        self.leave()

    # expr
    def _Str(self, tree):
        self.write(repr(tree.s))

    def _Name(self, t):
        self.write(t.id)

    def _Repr(self, t):
        self.write("`")
        self.dispatch(t.value)
        self.write("`")

    def _Num(self, t):
        self.write(repr(t.n))

    def _List(self, t):
        self.write("[")
        interleave(lambda: self.write(", "), self.dispatch, t.elts)
        self.write("]")

    def _ListComp(self, t):
        self.write("[")
        self.dispatch(t.elt)
        for gen in t.generators:
            self.dispatch(gen)
        self.write("]")

    def _GeneratorExp(self, t):
        self.write("(")
        self.dispatch(t.elt)
        for gen in t.generators:
            self.dispatch(gen)
        self.write(")")

    def _comprehension(self, t):
        self.write(" for ")
        self.dispatch(t.target)
        self.write(" in ")
        self.dispatch(t.iter)
        for if_clause in t.ifs:
            self.write(" if ")
            self.dispatch(if_clause)

    def _IfExp(self, t):
        self.write("(")
        self.dispatch(t.body)
        self.write(" if ")
        self.dispatch(t.test)
        self.write(" else ")
        self.dispatch(t.orelse)
        self.write(")")

    def _Dict(self, t):
        self.write("{")
        def writem((k, v)):
            self.dispatch(k)
            self.write(": ")
            self.dispatch(v)
        interleave(lambda: self.write(", "), writem, zip(t.keys, t.values))
        self.write("}")

    def _Tuple(self, t):
        self.write("(")
        if len(t.elts) == 1:
            (elt,) = t.elts
            self.dispatch(elt)
            self.write(",")
        else:
            interleave(lambda: self.write(", "), self.dispatch, t.elts)
        self.write(")")

    unop = {"Invert":"~", "Not": "not", "UAdd":"+", "USub":"-"}
    def _UnaryOp(self, t):
        self.write(self.unop[t.op.__class__.__name__])
        self.write("(")
        self.dispatch(t.operand)
        self.write(")")

    binop = { "Add":"+", "Sub":"-", "Mult":"*", "Div":"/", "Mod":"%",
                    "LShift":">>", "RShift":"<<", "BitOr":"|", "BitXor":"^", "BitAnd":"&",
                    "FloorDiv":"//", "Pow": "**"}
    def _BinOp(self, t):
        self.write("(")
        self.dispatch(t.left)
        self.write(" " + self.binop[t.op.__class__.__name__] + " ")
        self.dispatch(t.right)
        self.write(")")

    cmpops = {"Eq":"==", "NotEq":"!=", "Lt":"<", "LtE":"<=", "Gt":">", "GtE":">=",
                        "Is":"is", "IsNot":"is not", "In":"in", "NotIn":"not in"}
    def _Compare(self, t):
        self.write("(")
        self.dispatch(t.left)
        for o, e in zip(t.ops, t.comparators):
            self.write(" " + self.cmpops[o.__class__.__name__] + " ")
            self.dispatch(e)
            self.write(")")

    boolops = {ast.And: 'and', ast.Or: 'or'}
    def _BoolOp(self, t):
        self.write("(")
        s = " %s " % self.boolops[t.op.__class__]
        interleave(lambda: self.write(s), self.dispatch, t.values)
        self.write(")")

    def _Attribute(self,t):
        self.dispatch(t.value)
        self.write(".")
        self.write(t.attr)

    def _Call(self, t):
        self.dispatch(t.func)
        self.write("(")
        comma = False
        for e in t.args:
            if comma: self.write(", ")
            else: comma = True
            self.dispatch(e)
        for e in t.keywords:
            if comma: self.write(", ")
            else: comma = True
            self.dispatch(e)
        if t.starargs:
            if comma: self.write(", ")
            else: comma = True
            self.write("*")
            self.dispatch(t.starargs)
        if t.kwargs:
            if comma: self.write(", ")
            else: comma = True
            self.write("**")
            self.dispatch(t.kwargs)
        self.write(")")

    def _Subscript(self, t):
        self.dispatch(t.value)
        self.write("[")
        self.dispatch(t.slice)
        self.write("]")

    # slice
    def _Ellipsis(self, t):
        self.write("...")

    def _Index(self, t):
        self.dispatch(t.value)

    def _Slice(self, t):
        if t.lower:
            self.dispatch(t.lower)
        self.write(":")
        if t.upper:
            self.dispatch(t.upper)
        if t.step:
            self.write(":")
            self.dispatch(t.step)

    def _ExtSlice(self, t):
        interleave(lambda: self.write(', '), self.dispatch, t.dims)

    # others
    def _arguments(self, t):
        first = True
        nonDef = len(t.args)-len(t.defaults)
        for a in t.args[0:nonDef]:
            if first:first = False
            else: self.write(", ")
            self.dispatch(a)
        for a,d in zip(t.args[nonDef:], t.defaults):
            if first:first = False
            else: self.write(", ")
            self.dispatch(a),
            self.write("=")
            self.dispatch(d)
        if t.vararg:
            if first:first = False
            else: self.write(", ")
            self.write("*"+t.vararg)
        if t.kwarg:
            if first:first = False
            else: self.write(", ")
            self.write("**"+t.kwarg)

    def _keyword(self, t):
        self.write(t.arg)
        self.write("=")
        self.dispatch(t.value)

    def _Lambda(self, t):
        self.write("lambda ")
        self.dispatch(t.args)
        self.write(": ")
        self.dispatch(t.body)

    def _alias(self, t):
        self.write(t.name)
        if t.asname:
            self.write(" as "+t.asname)

def unparse(ast_obj):
    v = StringIO()
    Unparser(ast_obj, file=v)
    return v.getvalue()
# ------------------------------------------------------------------------------------------

def ast_parse(txt):
    ''' Returns ast tree of given text
    ARGS:
        txt: str - Pythonic text to be parsed in tree structure
    RETURNS 
    '''
    return ast.parse(txt.replace("\\\\","\\\\\\\\")).body[0].value

def tabify(txt):
    ''' Organizes a string of code by adding tabs and new lines when bracket contents get too large
    ARGS:
        txt: str - Python code to be organized
    '''
    # Max length of bracket contents before adding tabs to it
    BRACKET_LIMIT = 70
    # Largest string the function can handle before giving up and not tabifying it at all (as if you were to tabify it, it would be hundreds of lines long)
    FORMAT_LIMIT = 50000
    tab = "\t"
    char_to_indexes = {
        "(":[],
        "[":[],
        "{":[]
    }
    close_to_open = {
        ")":"(",
        "]":"[",
        "}":"{"
    }
    txt_masks = []
    masked_strings = []

    if len(txt) > FORMAT_LIMIT:
        return txt
    # Replace all characters within quotes with a token (as quotes with brackets in them can interfere with properly tabifying)
    txt = replace_quotes_and_comments(txt)
    i=0
    # Iterate through given text character by character
    while i < len(txt):
        # If character is open bracket, add index of bracket to bracket stack
        if txt[i] in char_to_indexes:
            char_to_indexes[txt[i]].append(i+1)
        # If character is close bracket, see if bracket contents need to be tabified
        elif txt[i] in close_to_open:
            # Pop from bracket stack
            start_i = char_to_indexes[close_to_open[txt[i]]].pop(-1)
            end_i = i
            new_txt = txt[start_i:end_i]
            # If bracket contents length exceed bracket limit, tabify
            if len(new_txt) > BRACKET_LIMIT:
                k=0
                while k<len(new_txt):
                    # Add tab after new line characters
                    if new_txt[k] == "\n":
                        new_txt = new_txt[:k+1] + tab + new_txt[k+1:]
                        k+=len(tab)
                    # Add new line and tab after commas
                    elif new_txt[k] == "," and new_txt[k+1] != "\n":
                        new_txt = new_txt[:k+1] + "\n" + tab + new_txt[k+1:]
                        k+=len("\n" + tab)
                    # Add new line and tab after '|' or '&'
                    elif new_txt[k] in ["|","&"] and new_txt[k-1] != "\n":
                        new_txt = new_txt[:k-1] + "\n" + tab + new_txt[k-1:]
                        k+=len("\n" + tab)
                    k+=1
                
                new_txt = "\n" + tab + new_txt + "\n"
            
            # Bracket contents replaced with mask of form '@@12----@@' so as not to interfere with tabbing the outer brackets
            mask_id = str(len(txt_masks))
            mask = "@@" + mask_id + "-"*(len(new_txt)-4-len(mask_id)) + "@@"
            txt_masks.append(mask)
            masked_strings.append(new_txt)
            txt = txt[:start_i] + mask + txt[end_i:]
            i = start_i + len(mask)
        i+=1
    # Replaces masks with their text
    while txt_masks:
        mask = txt_masks.pop(-1)
        masked_string = masked_strings.pop(-1)
        tabs = re.search("(?:^|\n)([ \t]*).*"+mask,txt).group(1)
        masked_string = masked_string.replace("\n","\n"+tabs)
        txt = txt.replace(mask,masked_string)
    txt = re.sub("\t[ ]+","\t",txt)
    txt = unreplace_quotes(txt)
    return txt

# ----------------------AST REPLACE CALLS---------------------------
function_ignore_list = ["col"]
# SPSS function to python equivalent and return type. Arguments in <> will be moved to correct location as dictated in python equivalent
# If the python equivalent below is a list containing multiple conversions, the code will choose the one it needs based on the return type eg. If it needs python code that returns an integer, it will choose the one with the integer return type
function_map = {
    "isstartstring(<substr>, <col>)":("(<col>.substr(lit(0),length(<substr>)) == <substr>).cast('int')",int),
    "isendstring(<substr>, <col>)":("udf(isendstring,IntegerType())(<substr>,<col>)",int),
    "ismidstring(<substr>, <col>)":("udf(ismidstring,IntegerType())(<substr>,<col>)",int),
    "hasstartstring(<col>, <substr>)":("(<col>.substr(lit(0),length(<substr>)) == <substr>).cast('int')",int),
    "hasmidstring(<col>, <substr>)":("udf(ismidstring,IntegerType())(<substr>,<col>)",int),
    "startstring(<len>, <col>)":("<col>.substr(lit(1),<len>)",str),
    "substring(<i>,<len>,<col>)":("<col>.substr(<i>,<len>)",str),
    "endstring(<i>,<col>)":("<col>.substr(length(<col>)-<i> + 1,<i>)",str),
    "allbutfirst(<i>,<col>)":("udf(allbutfirst)(<i>,<col>)",str),
    "allbutlast(<i>,<col>)":("udf(allbutlast)(<i>,<col>)",str),
    "subscrs(<i>, <col>)":("<col>.substr(<i>,lit(1))",str),
    "member(<col>,<vals>)":("<col>.isin(<vals>)",bool),
    "@NULL(<col>)":("(<col>.isNull() | (<col>.cast('string') == '$null$') | (<col>.cast('string') == 'NaN'))",bool),
    "@BLANK(<col>)":("lit(False)",bool),
    "@TODAY":("getTodaysDate(parent_config_file)",None),
    "@OFFSET(<col>,<offset>)":("lag(<col>,<offset>).over(Window.orderBy(monotonically_increasing_id()))",None),
    "datetime_now": ("getTodaysDate(parent_config_file)",None),
    "@INDEX":("row_number().over(Window.orderBy(monotonically_increasing_id()))",None),
    "issubstring(<substr>, <col>)":[
        ("udf(substr_index,IntegerType())(<substr>,<col>)",int),
        ("<col>.contains(<substr>)",bool),
    ],
    "issubstring(<substr>, <i>, <col>)":[
        ("udf(substr_index,IntegerType())(<substr>,<col>,<i>)",int),
        ("<col>.substr(<i>,length(<col>)).contains(<substr>)",bool),
    ],
    "hassubstring(<col>,<substr>)":[
        ("udf(substr_index,IntegerType())(<substr>,<col>)",int),
        ("<col>.contains(<substr>)",bool),
    ],
    "datetime_timestamp(<year>,<month>,<day>,<hour>,<minute>,<second>)":("udf(datetime_timestamp,TimestampType())(<year>,<month>,<day>,<hour>,<minute>,<second>)",None),
    "datetime_timestamp(<seconds>)":("udf(seconds_to_datetime,TimestampType())(<seconds>)",None),
    "datetime_year(<col>)":("year(udf(to_date_infer,DateType())(<col>))",int),
    "datetime_month(<col>)":("month(udf(to_date_infer,DateType())(<col>))",int),
    "datetime_day(<col>)":("dayofmonth(udf(to_date_infer,DateType())(<col>))",int),
    "datetime_in_seconds(<col>)":("unix_timestamp(udf(to_datetime_infer,TimestampType())(<col>))",int),
    "to_timestamp(<col>)":("udf(to_datetime_infer,TimestampType())(<col>)",None),
    "date_months_difference(<col1>,<col2>)":("datediff(udf(to_date_infer,DateType())(<col2>), udf(to_date_infer,DateType())(<col1>)) / lit(30.4375)",int),
    "date_years_difference(<col1>,<col2>)":("datediff(udf(to_date_infer,DateType())(<col2>), udf(to_date_infer,DateType())(<col1>)) / lit(365.25)",int),
    "undef":("None",None),
    "to_string(<col>)":("udf(to_string)(<col>)",str),
    "to_integer(<col>)":("<col>.cast(IntegerType())",int),
    "to_number(<col>)":("<col>.cast(DoubleType())",float),
    "date_days_difference(<col1>, <col2>)":("datediff(udf(to_date_infer,DateType())(<col2>), udf(to_date_infer,DateType())(<col1>))",int),
    "uppertolower(<col>)":("lower(<col>)",str),
    "lowertoupper(<col>)":("upper(<col>)",str),
    "length(<col>)":("length(<col>)",int),
    "trim(<col>)":("udf(strip)(<col>)",str),
    "replace(<substr1>,<substr2>,<col>)":("udf(replace)(<substr1>,<substr2>,<col>)",str),
    "stripctrlchars(<col>)":("regexp_replace(<col>,'[\\x00-\\x1F]+','')",str),
    "trimend(<col>)":("udf(rstrip)(<col>)",str),
    "trimstart(<col>)":("udf(lstrip)(<col>)",str),
    "count_substring(<col>,<substr>)":("size(split(<col>, <substr>))-lit(1)",int),
    "textsplit(<col>,<i>,<char>)":("split(<col>,<char>).getItem(<i>-lit(1))",None),
    "unicode_char(<col>)":("udf(lambda num:chr(num))(<col>)",str),
    "sum_n(<args>)":("udf(sum_n)(<args>)",float),
    "max(<col1>,<col2>)":("greatest(<col1>,<col2>)",int),
    "min(<col1>,<col2>)":("least(<col1>,<col2>)",int),
    "fracof(<col>)":("<col> - <col>.cast(IntegerType())",float),
    "round(<col>)":("_round(<col>).cast(IntegerType())",int),
    "substring_between(<i>,<j>,<col>)":("<col>.substr(<i>,<j>-<i> + lit(1))",str),
    "to_real(<col>)":("<col>.cast(DoubleType())",float),
    "intof(<col>)":("<col>.cast(IntegerType())",int),
    "to_date(<col>)":("udf(to_date_infer,DateType())(<col>)", None),
    "datetime_date(<year>,<month>,<day>)":("udf(datetime_date,DateType())(<year>,<month>,<day>)",None),
    "date_in_days(<date>)":("datediff(udf(to_date_infer,DateType())(<date>),to_date(lit('1900-01-01')))",int),
    "datetime_month_name(<col>)":("udf(num_to_month)(<col>)",str),
    "datetime_month_short_name(<col>)":("udf(num_to_month)(<col>).substr(1,3)",str),
    "cdf_normal(norm_random,<mean>,<std>)":("udf(lambda mean, std: float(norm(mean, std).cdf(norm(0, 1).rvs())))(<mean>,<std>)",float),
}
# Refactors function_map into more verbose form
for spss_func in list(function_map.keys()):
    spss_func_tuple = re.findall("([^\(\s,\)]+)",spss_func)
    spss_func_name = spss_func_tuple[0]
    spss_func_args = spss_func_tuple[1:]
    if type(function_map[spss_func]) != list:
        python_func = function_map[spss_func][0]
        python_func_return_type = function_map[spss_func][1]
        function_map[(spss_func_name,len(spss_func_args))] = (spss_func_args,python_func,python_func_return_type)
    else:
        function_map[(spss_func_name,len(spss_func_args))] = (spss_func_args,function_map[spss_func])
    function_map.pop(spss_func)

def spss_lists_to_python_lists(text):
    '''Converts SPSS list to python list
    SPSS lists have much looser requirements than python lists
        - Items can be seperated by spaces as well as commas
        - Strings don't need to have quotes
        - Strings with quotes don't need to be seperated by anything at all
    This function will add quotes to unquoted strings, and place commas between all list items
    '''
    new_text = ""
    end_ind = 0
    # Search for list in text identified by square brackets
    match = re.search("\[[^\]]*\]", text)
    while match:
        match_text = match.group()
        start_ind = match.start() + end_ind
        new_text += text[end_ind:start_ind]
        match_text = match_text[1:-1].strip()

        # add commas between items
        match_text = "[" + re.sub("(?:\s+,?\s*|,?\s+)",", ",match_text) + "]"
        #add quotes to all strings that don't already have quotes. if it starts with an integer, then it must be an integer not a string (by SPSS rules)
        match_text = re.sub("([, \[])([^\d,\s\]\$][^,\s\]\$]*)",r"\1'\2'",match_text)
        new_text += match_text
        end_ind = match.end() + end_ind
        
        # Search for next list in text
        match = re.search("\[[^\]]*\]", text[end_ind:])
    new_text += text[end_ind:]
    return new_text
        
id_to_string = {}
def replace_quotes_and_comments(text):
    "Replaces quotes within a string with tokens, so that regex operations can be applied to the string without matching the body of the quotes"
    new_text = ""
    i = 0
    # Iterate through text character by character
    while i < len(text):
        char = text[i]
        # If quote is found, replace quote body with token
        if char in ['"','\'']:
            # Only except quotes preceded by an even number of backslashes, as this is technically a character, not a quote
            end_i = re.search(r'[^\\](?:\\\\)*' + char, text[i:]).end() + i
            substr = text[i+1:end_i-1]
            # Assigns token for text of form '$$12$$$$$$' with a length equal to the length of the text, so as not to interfere with properly tabifying the text later
            new_substr = "$$" + str(len(id_to_string)) + "$"*(len(substr)-6) + "$$"
            # If text is unicode, convert to string without interfering with text backslashes
            if type(text) == unicode:
                substr = str(repr(substr))[2:-1].replace("\\\\","\\")
                substr = re.sub("((?:^|[^\\\\])(?:\\\\\\\\)*)'","\\1\\\\'",substr)
                id_to_string[new_substr] = char + substr + char
            else:
                id_to_string[new_substr] = char + str(substr) + char
            new_text += new_substr
            # Edge case: two quoted list items have no character seperating them: adds a space
            if end_i < len(text) and text[end_i] in ['"','\'']:
                new_text += " "
            i=end_i
        # If comment is found, ignore everything up to new line character
        elif char == "#":
            # Jump to end of comment
            i += re.search('(?:\n|$)', text[i:]).end()
        else:
            new_text += char
            i+=1
    return new_text
def unreplace_quotes(text):
    '''Replaces tokens with the corresponding string in id_to_string'''
    for string_id, string in id_to_string.items():
        text = text.replace(string_id, string)
    return text

def Constant(value,kind=None):
    if type(value) in [int,float]:
        return ast.Num(n=value)
    elif type(value) == str:
        return ast.Str(s=value)
    else:
        raise Exception("Not implemented (Constant)")
# ast.Attribute does not work. Can only get Attribute object using ast.parse
def Attribute(value = None, attr = None):
    a = ast.parse("a.a").body[0].value
    a.value = value
    a.attr=attr
    return a

def cast_ast(obj, new_type, old_type = None):
    '''Adds code to given ast object (obj) to cast the code to the given type (new_type)
    ARGS:
        obj: ast.AST or str
        new_type: type
        old_type: type
    '''
    if old_type != new_type:
        obj_ast = isinstance(obj, ast.AST)
        # Convert ast object to string
        if obj_ast:
            obj_str = "(" + unparse(obj) + ")"
        else:
            obj_str = obj

        # The code that needs to be added to cast the ast object to the correct type
        type_map = {str:".cast(StringType())",int:".cast(IntegerType())",float:".cast(DoubleType())",bool:".cast(BooleanType())"}
        # SPSS infers integers as boolean True if they are >1, and False if they are 0. In python this is instead stated in the code explicitly
        if old_type in [float,int] and new_type == bool:
            obj_str += ">=lit(1)"
        # Add code to cast obj to new_type as dictated in type_map
        elif new_type in type_map:
            obj_str += type_map[new_type]
        else:
            raise Exception("Undefined type cast: from " + str(old_type) + " to " + str(new_type))
        
        # If obj was originally an ast object, convert string back to ast object
        if obj_ast:
            obj = ast.parse(obj_str).body[0].value
        else:
            obj = obj_str
    return obj

def replace_calls_ast(parent_node, cast_bool = False):
    '''Given ast object, replaces spss code with python code
    ARGS:
        parent_node: ast.AST - ast object to be modified into python
        cast_bool: bool - Whether to add layer ontop that ensures the result is of boolean type. This is useful if the given expression is supposed to be a condition, such as in an if statement
    '''
    return_type = None
    # Iterates through each attribute in object
    for attr, val in ast.iter_fields(parent_node):
        # If val is a list, pairs each element in the list with its index in the list. Otherwise, puts val in list form like so: [(val,0)]. This makes it easier to iterate through
        val_list = val if type(val) in [list,ast.astlist] else [val]
        val_list = [(value,i) for value,i in zip(val_list,range(len(val_list)))]
        node_list = [(node,i) for node,i in val_list if isinstance(node, ast.AST)]
        # Iterates over each element in val
        for node, i in node_list:
            # cast the child node of the current node to boolean if cast_bool is enabled and the node is not already a comparison (ast.Compare), boolean operation (ast.BoolOp), and its not an if statement (ast.If) or function call (ast.Call)
            cast_child_bool = (type(parent_node) == ast.Module and attr == "body" and cast_bool) or (type(parent_node) == ast.Expr and cast_bool)
            cast_child_bool &= type(node) not in [ast.Compare, ast.BoolOp, ast.If, ast.Call]
            # The current node should be in boolean form irregardless of the value of cast_bool if:
            #   - The node is one of the sides of a boolean operation eg. A | B
            #   - The node is the condition of an if statement
            #   - The node is in a 'not' statement
            #   - The node is a issubstring/hassubstring call in the following expression: issubstring(<args>) ><= 0
            cast_bool |= (
                (type(parent_node) == ast.BoolOp and attr == "values") 
                or (type(parent_node) == ast.If and attr == "test" and type(node) != ast.If) 
                or (type(parent_node) == ast.UnaryOp and type(parent_node.op) == ast.Not and attr == "operand")
                or (type(parent_node) == ast.Compare and type(parent_node.left) == ast.Call and type(parent_node.left.func) == ast.Name and parent_node.left.func.id in ["issubstring","hassubstring"] and type(parent_node.comparators[0]) == ast.Num and parent_node.comparators[0].n == 0 and i==0)
            )

            # Runs depth first search on ast tree. This means the tree will be converted from the leaves up
            return_type = replace_calls_ast(node, cast_child_bool)

            # This accounts for an edge case when many successive replace calls are called in a row (eg. replace(replace(replace(...)))). Converting this to pyspark verbatim will crash pyspark (if there are enough of them). Instead, this looks for an opportunity to compress the expression using regex
            # Specifically, if two successive replace statements replace a substring with the same substring such as in replace('a',' ',replace('b',' ',...)), this can be compressed into re.sub('(?:a|b)',' ',...)
            condense_replace = False
            # Defines variables to be used later
            if (
                # This ensures the outer replace statement is a replace call
                type(node) == ast.Call 
                and type(node.func) == ast.Name 
                and node.func.id == "replace" 
                # This ensures the inner replace statement is a python converted replace call
                and type(node.args[2]) == ast.Call 
                and type(node.args[2].func) == ast.Call 
                and type(node.args[2].func.func) == ast.Name 
                and node.args[2].func.func.id == "udf" 
                and type(node.args[2].func.args[0]) == ast.Name 
                and node.args[2].func.args[0].id in ["replace","regex_replace"] 
                # This checks that none of the parameters are columns
                and not(type(node.args[0]) == ast.Call and type(node.args[0].func) == ast.Name and node.args[0].func.id == "col")
                and not(type(node.args[1]) == ast.Call and type(node.args[1].func) == ast.Name and node.args[1].func.id == "col")
                and not(type(node.args[2].args[0]) == ast.Call and type(node.args[2].args[0].func) == ast.Name and node.args[2].args[0].func.id == "col")
                and not(type(node.args[2].args[1]) == ast.Call and type(node.args[2].args[1].func) == ast.Name and node.args[2].args[1].func.id == "col")
                # This checks if the replacements substrings are the same
                and unparse(node.args[2].args[1]) == unparse(node.args[1])
            ):
                condense_replace = True

                # Unparses the inner replace matching string
                regex_substr = unparse(node.args[2].args[0].args[0]).strip()[1:-1]
                # If the inner replace matching string is a list eg. (?:a|b|...), opens it up so that it can be appended to
                if regex_substr[:3] == "(?:" and regex_substr[-1] == ")":
                    regex_substr = regex_substr[3:-1].replace("\\\\","\\")
                else:
                    regex_substr = re.escape(regex_substr)
                # Matching string of outer replace
                other_substr = unparse(node.args[0].args[0]).strip()[1:-1]
                # Replacement value. This has already been verified to be the same for both replacement calls
                replace_val = unparse(node.args[1].args[0]).strip()[1:-1]
                
                # This breaks both the matching substrings and the replacement substring into a set of characters. If there is any overlap between the regex_substr_set/other_substr_set and the replace_val_set then the statements cannot be compressed, because the statements have to be conducted in the specified order to procure the same result
                # Sometimes these substrings will contain unicode(\uXXXX) or hexcode(\xXX) characters. Because this is python 2.5, not all of these characters will be known, and the character's codes are used in their place. This regex ensures that the characters of the code aren't broken up in the set, but are instead all treated as one thing
                regex_substr_set = set(re.findall("(?:\\\\x\w\w|\\\\\\\\u[\da-f]{4})",regex_substr)) | set(re.sub("(?:\\\\x\w\w|\\\\\\\\u([\da-f]{4}))","",regex_substr))
                other_substr_set = set(re.findall("(?:\\\\x\w\w|\\\\\\\\u[\da-f]{4})",other_substr)) | set(re.sub("(?:\\\\x\w\w|\\\\\\\\u([\da-f]{4}))","",other_substr))
                replace_val_set = set(re.findall("(?:\\\\x\w\w|\\\\\\\\u[\da-f]{4})",replace_val)) | set(re.sub("(?:\\\\x\w\w|\\\\\\\\u([\da-f]{4}))","",replace_val))

                other_substr_escaped = re.escape(other_substr)
                replace_val_escaped = re.escape(replace_val)
                # Edge case: If the only location of the replacement substring is at the start and the end of the inner matching string, then the order doesn't matter and the calls can be compressed eg. replace('abcdea','a',...) can be compressed
                regex_sandwiched = (
                    len(regex_substr) >= len(replace_val_escaped) > 0
                    and regex_substr[:len(replace_val_escaped)] == regex_substr[-len(replace_val_escaped):] == replace_val_escaped
                    and (
                        len(regex_substr) < 2*len(replace_val_escaped)
                        or regex_substr[len(replace_val_escaped):2*len(replace_val_escaped)] != replace_val_escaped != regex_substr[-2*len(replace_val_escaped):-len(replace_val_escaped)] 
                    )
                )
                # Same edge case but for the outer matching string
                other_sandwiched = (
                    len(other_substr_escaped) >= len(replace_val_escaped) > 0
                    and other_substr_escaped[:len(replace_val_escaped)] == other_substr_escaped[-len(replace_val_escaped):] == replace_val_escaped
                    and (
                        len(other_substr_escaped) < 2*len(replace_val_escaped)
                        or other_substr_escaped[len(replace_val_escaped):2*len(replace_val_escaped)] != replace_val_escaped != other_substr_escaped[-2*len(replace_val_escaped):-len(replace_val_escaped)] 
                    )
                )
            # <COLUMN_NAME> -> col("<COLUMN_NAME")
            # This puts col(...) around column names
            if type(node) == ast.Name and attr != "func" and not (type(parent_node) == ast.Call and parent_node.func.id in ["col","lit"]):# and node.id not in ["datetime_now"]:
                name = unparse(node).strip()
                # Replaces __AT__ placeholder with @ symbol
                if name in ["__AT__NULL","__AT__BLANK", "__AT__TODAY", "__AT__INDEX","__AT__OFFSET"]:
                    name = "@" + name[6:]
                # If the column name is in the function map, then this is treated as a zero-argument function called (like @NULL), not a column name. It is mapped accordingly
                if (name,0) in function_map:
                    args, name, return_type = function_map[(name,0)]
                    new_node = ast.parse("lit(%s)"%(name)).body[0].value
                else:
                    if "." in name:
                        name = "`" + name + "`"
                    # Handles backslashes
                    name = re.sub("((?:^|[^\\\\])(?:\\\\\\\\)*)'","\\1\\\\'",name)
                    # Adds col()
                    new_node = ast.parse("col('%s')"%(name)).body[0].value
            # '__COL__<COLUMN_NAME' -> col("<COLUMN_NAME")
            # Strings are interpretted as column names if they are prefaced with '__COL__', as inserted in replace_calls. col() is added to them
            elif type(node) == ast.Str and node.s[:7] == "__COL__":
                name = node.s[7:]
                if "." in name:
                    name = "`" + name + "`"
                name = re.sub("((?:^|[^\\\\])(?:\\\\\\\\)*)'","\\1\\\\'",name)
                new_node = ast.parse("col('%s')"%(name)).body[0].value
            #"<const>" -> "lit(<const>)"
            # Adds lit(...) to constants (with the below exceptions)
            elif (
                type(node) in [ast.Num, ast.Str]
                # Don't add lit() to the character argument of the textsplit function
                and not (type(parent_node) == ast.Call and parent_node.func.id == "textsplit" and attr == "args" and i == 2)
                # Don't add lit() to the arguments of stripctrlchars, count_substring, or @OFFSET
                and not (type(parent_node) == ast.Call and parent_node.func.id in ["stripctrlchars","count_substring","__AT__OFFSET"])
                # Don't add lit() to elements of a list
                and not (type(parent_node) == ast.List)
                # Don't add lit() if the constant is already surrounded by a lit, or a col
                and not (type(parent_node) == ast.Call and parent_node.func.id in ["col","lit"])
            ):
                const = unparse(node).strip()

                const = "lit(%s)"%(const)

                # SPSS automatically interprets a date in string form as a date, not a string. Strings must be cast to dates explicitly in pyspark. String dates are supplied to a to_date function to cast it, as long as they are not an argument of one of the below functions
                if not (type(parent_node) == ast.Call and parent_node.func.id in ["replace","isstartstring","isendstring","ismidstring","hasmidstring","startstring","substring","endstring","allbutfirst","allbutlast","subscrs","issubstring","hassubstring","uppertolower","lowertoupper","trim","trimend","trimstart","count_substring","substring_between"]):
                    # Infers the format of the date (dd/MM/yyyy, yyyy/MM/dd, yyyy-MM-dd, or yyyy-MM-dd HH:mm:ss) using regex
                    if type(const) == str and re.search("^lit\(['\"]\d\d/\d\d/\d\d\d\d['\"]\)$",const):
                        const = "to_date(%s, format='dd/MM/yyyy')"%(const)
                    elif type(const) == str and re.search("^lit\(['\"]\d\d\d\d/\d\d/\d\d['\"]\)$",const):
                        const = "to_date(%s, format='yyyy/MM/dd')"%(const)
                    elif type(const) == str and re.search("^lit\(['\"]\d\d\d\d-\d\d-\d\d['\"]\)$",const):
                        const = "to_date(%s, format='yyyy-MM-dd')"%(const)
                    elif type(const) == str and re.search("^lit\(['\"]\d\d\d\d-\d\d-\d\d \d\d:\d\d:\d\d['\"]\)$",const):
                        const = "to_timestamp(%s, format='yyyy-MM-dd HH:mm:ss')"%(const)

                new_node = ast.parse(const).body[0].value
                if type(node) == ast.Num:
                    return_type = int
                else:
                    return_type = str
            # Compress embedded replace calls with one regexp_replace call
            # replace(<substr1>,<substr>,regexp_replace(<col>,<substr2>,<substr>)) -> regexp_replace(<col>,'(?:<substr1>|<substr2>)',<substr>)
            # The checks to see if it can be compressed is done above
            elif (condense_replace and ((not (regex_substr_set & other_substr_set) and not (regex_substr_set & replace_val_set) and not (other_substr_set & replace_val_set)) or (regex_sandwiched and other_sandwiched))):
                regex_substr = unparse(node.args[2].args[0].args[0]).strip()[1:-1]
                # If the inner replace matching string is a list eg. (?:a|b|...), opens it up so that it can be appended to
                if regex_substr[:3] == "(?:" and regex_substr[-1] == ")":
                    regex_substr = regex_substr[3:-1]
                else:
                    regex_substr = re.escape(regex_substr)
                # Puts substrings in regex list
                regex_substr = "(?:" + regex_substr + "|" + re.escape(unparse(node.args[0].args[0]).strip()[1:-1]) + ")"
                col_val = unparse(node.args[2].args[2]).strip()
                replace_val = unparse(node.args[1].args[0]).strip()[1:-1]
                # Puts into one regex_replace function call
                new_node = ast.parse(
                    "udf(regex_replace)(lit(" + cast(regex_substr)  + "),lit(" + cast(replace_val) + ")," + col_val + ")"#.replace("\\\\","\\\\\\\\")
                ).body[0].value
                return_type = str
            #<spss_func>(<arg1>,<arg2>,...) -> <python_func>(<python_arg1>,<python_arg2>,...)
            # Maps SPSS functions to python functions using the function_map
            elif type(node) == ast.Call and type(node.func) == ast.Name and node.func.id not in function_ignore_list and (node.func.id != "to_date" or type(node.args[0]) != ast.Call or type(node.args[0].func) != ast.Name or node.args[0].func.id != "to_date"):
                spss_func_name = node.func.id
                num_args = len(node.args)
                if spss_func_name in ["__AT__NULL","__AT__BLANK", "__AT__TODAY", "__AT__INDEX","__AT__OFFSET"]:
                    spss_func_name = "@" + spss_func_name[6:]
                # Key in function_map. The function name with the number of arguments
                key = (spss_func_name,num_args)
                if key not in function_map:
                    raise Exception("Function '" + spss_func_name + "' with " + str(len(node.args)) + " arguments is not implemented yet")
                
                # Gets python conversion of spss function (if there is only one possible option)
                if type(function_map[key][1]) != list:
                    args, python_func, return_type = function_map[key]
                # If there is more than one conversion option and cast_bool is enabled, chooses the one that returns a bool type, otherwise, chooses the first non-bool returning option
                else:
                    args, func_list = function_map[key]
                    for python_func, return_type in func_list:
                        if (cast_bool and return_type == bool) or (not cast_bool and return_type != bool):
                            break
                # For each argument in the spss function, replaces it in the python translation with the actual argument in the current context
                # eg. max(<col1>,<col2>) might replace greatest(<col1>,<col2>) with greatest(col('SALARY1'),col('SALARY2'))
                for j in range(len(args)):
                    # Argument placeholder string eg. <col1>
                    arg = args[j]
                    # Corresponding argument ast object
                    arg_val = node.args[j]
                    # Object unparsed into a string
                    arg_val = unparse(arg_val).strip()
                    # The placeholder string is replaced with the unparsed ast object
                    python_func = python_func.replace(arg, arg_val)
                # Edge case: getTodaysDate(parent_config_file) needs a lit() around it
                if spss_func_name in ["@TODAY"]:
                    python_func = "lit(" + python_func + ")"
                # Parses back into an ast object
                new_node = ast.parse(python_func).body[0].value
                # If the function needs to be casted to boolean, uses cast_ast to cast to boolean
                if cast_bool:
                    new_node = cast_ast(new_node, bool, return_type)
                    return_type = bool
            # to_date(to_date(...)) -> to_date(...)
            # Cleanup: successive to_date calls can be compressed to just one
            elif type(node) == ast.Call and type(node.func) == ast.Name and node.func.id == "to_date" and type(node.args[0]) == ast.Call and type(node.args[0].func) == ast.Name and node.args[0].func.id == "to_date":
                new_node = node.args[0]
            # <col1> >< <col2> -> concat(<col1>,<col2>)
            # Note: concat operator ('><') replaced with right bit shift operator ('>>') to be able to be interpretted by ast
            elif type(node) == ast.BinOp and type(node.op) == ast.RShift:
                node_left_python = unparse(node.left).strip()
                node_right_python = unparse(node.right).strip()
                # If the inner argument is already a concat call, then this compresses the inner and outer arguments into one concat call
                if node_left_python[:7] == "concat(" and node_left_python[-1] == ")":
                    node_left_python = node_left_python[7:-1]
                new_node = ast.parse("concat(%s, %s)"%(node_left_python,node_right_python)).body[0].value
                return_type = str
            # <col>.contains(<substr>) >/!= 0 -> <col>.contains(<substr>), <col>.contains(<substr>) <=/== 0 -> not(<col>.contains(<substr>))
            # In SPSS, the hassubstring/issubstring methods return the number of instances of that substring in the argument. Often this is unneeded; all that is need is whether or not the substring is in the argument. As such, the output is often then compared with 0 to see if the substring occurs a non-zero number of times. Since the .contains method already does this, this section strips away the comparison if it is to the number 0
            elif type(node) == ast.Compare and type(node.left) == ast.Call and type(node.left.func) == type(Attribute()) and node.left.func.attr == "contains" and type(node.comparators[0]) == ast.Call and type(node.comparators[0].func) == ast.Name and node.comparators[0].func.id == "lit" and type(node.comparators[0].args[0]) == ast.Num and node.comparators[0].args[0].n == 0:
                if type(node.ops[0]) in [ast.Gt,ast.NotEq]:
                    prefix = ""
                else:
                    prefix = "not "
                cond = unparse(node.left).strip()
                new_node = ast.parse(prefix + cond).body[0].value
                return_type = bool
            # <col> == lit(None) -> isnull(<col>), <col> != lit(None) -> not(isnull(<col>))
            # Comparisons to null values are instead dont with the isnull function
            elif type(node) == ast.Compare and type(node.comparators[0]) == ast.Call and type(node.comparators[0].func) == ast.Name and node.comparators[0].func.id == "lit" and type(node.comparators[0].args[0]) == ast.Name and node.comparators[0].args[0].id == "None":
                if type(node.ops[0]) == ast.Eq:
                    prefix = ""
                elif type(node.ops[0]) == ast.NotEq:
                    prefix = "not "
                left_val = unparse(node.left).strip()
                new_node = ast.parse(prefix + "isnull(" + left_val + ")").body[0].value
                return_type = bool
            # Replace multiple locate conditions with one regexp_extract condition
            #(locate(<substr1>, <col>) > 0) | (locate(<substr2>, <col>) > 0)":"regexp_extract(<col>, '(<substr1>|<substr2>)', 1) != ''
            elif type(node) == ast.BoolOp:
                cond_info_to_substrs = {}
                conds = []

                # Locate expressions all being compared with the number 0 can be compressed as shown above. This checks what each locate function is being compared to, what the comparison is (==,!=,>,etc.), and groups expressions together accordingly
                for cond in node.values:
                    # locate(<substr1>, <col>) == 0
                    if type(cond) == ast.Compare and type(cond.left) == ast.Call and type(cond.left.func) == ast.Name and cond.left.func.id == "locate" and type(cond.left.args[0]) == ast.Str and type(cond.comparators[0]) == ast.Call and type(cond.comparators[0].func) == ast.Name and cond.comparators[0].func.id == "lit" and type(cond.comparators[0].args[0]) == ast.Num and cond.comparators[0].args[0].n == 0:
                        # > 0 is the same as != 0 in this context
                        if cond.comparators[0].args[0].n == 0 and type(cond.ops[0]) in [ast.Gt,ast.NotEq]:
                            new_op = "!="
                        else:
                            new_op = "=="
                        # Locate column paired with the operation of the locate expressions
                        cond_info = (unparse(cond.left.args[1]),new_op)
                        # Maps this key to a list of all substrings with the same column and operation, to be grouped into one regexp_extract expression
                        if cond_info not in cond_info_to_substrs:
                            cond_info_to_substrs[cond_info] = [unparse(cond.left.args[0]).strip()[1:-1]]
                        else:
                            cond_info_to_substrs[cond_info].append(unparse(cond.left.args[0]).strip()[1:-1])
                    # Don't include lit(False) in condition
                    elif not(type(cond) == ast.Call and type(cond.func) == ast.Name and cond.func.id == "lit" and type(cond.args[0]) == ast.Name and cond.args[0].id == "False"):
                        conds.append(cond)
                # Adds new compressed regexp_extract expression containing the substring arguments of multiple locate expressions all with |'s between them
                for ((col_val, new_op),substrs) in cond_info_to_substrs.items():
                    substr_regex = "|".join([re.escape(str(substr).replace("\\","\\\\")) for substr in substrs])
                    conds.append(ast.parse("regexp_extract(" + col_val + ", '(" + substr_regex + ")', 1) " + new_op + " ''").body[0].value)
                new_node = ast.BoolOp(op=node.op,values=conds)
                return_type = bool
            # if <cond>: <expr1> else: <expr2> -> when(<cond>,<expr1>).otherwise(<expr2>)
            # Maps if statements to when conditions. Compresses nested if statements eg. if <cond1>: <expr1> else: if <cond2>: <expr2> -> when(<cond1>,<expr1>).when(<cond2>,<expr2>)
            elif type(node) == ast.If and type(parent_node) != ast.If:
                arg1 = unparse(node.test).strip()
                arg2 = unparse(node.body[0].value).strip()
                new_node = ast.parse("when(%s,%s)"%(arg1,arg2)).body[0].value
                cur_node = node.orelse[0]
                # Adds new .when statement for each nested if statement in the else argument
                while type(cur_node) == ast.If:
                    a = unparse(new_node).strip()
                    arg1 = unparse(cur_node.test).strip()
                    arg2 = unparse(cur_node.body[0].value).strip()
                    new_node = ast.parse("%s.when(%s,%s)"%(a,arg1,arg2)).body[0].value
                    cur_node = cur_node.orelse[0]
                # Final 'else' converted to .otherwise statement
                arg1 = unparse(new_node).strip()
                arg2 = unparse(cur_node.value).strip()
                new_node = ast.parse("%s.otherwise(%s)"%(arg1,arg2)).body[0].value
                # If statements have to be within an Expression object
                new_node = ast.Expr(value=new_node)
                return_type = None
            # sum_n(<col1>,<col2>) -> udf(sum_n)([<col1>,<col2>])
            # In other words the python conversion of the sum_n function needs the arguments in a list
            elif type(node) == ast.List and type(parent_node) == ast.Call and type(parent_node.func) == ast.Name and parent_node.func.id == "sum_n":
                new_list = "array("
                for arg in node.elts:
                    name = unparse(arg)
                    if "." in name:
                        name = "`" + name + "`"
                    new_list += "col(" + name + "),"
                new_list += ")"
                new_node = ast.parse(new_list).body[0].value
            else:
                new_node = node
            
            # Replaces old ast object within the ast tree structure with new ast object
            if type(val) in [list,ast.astlist]:
                val[i] = new_node
            elif type(val) == ast.List:
                val.elts[i] = new_node
            else:
                setattr(parent_node,attr,new_node)
    return return_type

def replace_calls(cond, field_name="__AT__FIELD", cast_bool = False, fields = []):
    '''Converts SPSS conditions into equivalent python condition using function_map defined above and the function replace_calls_ast
    Specifically, this function applies regex substitutions to get the condition into a form that can be parsed by ast, and then sends the ast object to replace_calls_ast
    ARGS:
        cond: str - condition to be converted to python
        field_name: str - In the case a condition uses the '@FIELD' token, this is where its specified what the actual fieldname is supposed to be
        cast_bool: bool - Whether to cast the converted condition to a boolean, in the case it produces an integer that is supposed to be interpretted as a boolean
        fields: list[str] - The current fields in the node. This is only needed if the condition references one of these fields using single quotes, which is meant to refer to a field name, not a string
    '''
    # Replaces quotes and comments with tokens so that regex can be applied to text without matching characters in the quotes/comments
    cond = replace_quotes_and_comments(cond)
    # If a string uses single quotes, and the string is a current field name, SPSS interprets the string as a field name, not a string. In this case, the __COL__ prefix is added to these strings so that they are later interpreted by the parser as a field name, not a string
    for _id,str_val in id_to_string.items():
        if str_val[0] == '\'' and re.sub("((?:^|[^\\\\])(?:\\\\\\\\)*)\\\\'","\\1'",str_val[1:-1]) in fields:
            id_to_string[_id] = '\'__COL__' + str_val[1:-1] + '\''

    cond = cond.strip()
    cond = cond.replace("\n", " ")
    cond = cond.replace("\r", "")
    # Puts SPSS lists in proper python form
    cond = spss_lists_to_python_lists(cond)
    cond = re.sub("([^/><=])=([^=])",r"\1==\2",cond)
    cond = re.sub("/=","!=",cond)
    # Concat operator >< does not exist in python and cannot be parsed. This replaces it with different placeholder operator >> that can be parsed
    cond = cond.replace("><",">>")
    # Python variables cannot start with @ symbol. @ symbol is replaced with __AT__ token
    cond = re.sub("@(BLANK|NULL|TODAY|INDEX|OFFSET)",r"__AT__\1", cond)
    # Field names with periods in them need `` quotes to function correcly in pyspark
    if "." in field_name:
        field_name = "`" + field_name + "`"
    # Replace SPSS @FIELD with the actual field name its referencing
    cond = cond.replace("@FIELD", "col(" + field_name + ")")
    # Python if statements don't have 'then' keyword. This is instead replaced with a colon
    cond = re.sub("([^\w]\d*)then([^\w])",r"\1:\n\t\2",cond)
    # Replaces SPSS 'else if' with 'elif'
    cond = re.sub("([^\w]\d*)else\s*if([^\w])",r"\1\nelif \2",cond)
    # Adds colon to SPSS 'else' keyword
    cond = re.sub("([^\w]\d*)else([^\w])",r"\1\nelse:\n\t\2",cond)
    # Removes all SPSS 'endif's, as this is not used in python
    cond = re.sub("([^\w]\d*)endif(?=[^\w]|$)",r"\1",cond)
    # Replaces mod function calls with python equivalent %
    cond = re.sub("([^\w])mod([^\w])",r"\1%\2",cond)
    # Unreplaces the previously replaced quote tokends
    cond = unreplace_quotes(cond)

    # Converts condition into tree form using ast
    module = ast.parse(cond)
    # Replaces all spss function calls with their python equivalent
    return_type = replace_calls_ast(module, cast_bool)
    # Converts tree back into string
    output_cond = unparse(module).strip()

    # Re-replace quotes with tokens again for additional regex processing
    output_cond = replace_quotes_and_comments(output_cond)
    # 'or', 'and', and 'not' replaced with BinOp equivalent |, &, and ~. This is done after the ast parsing because this version of ast cannot handle 3+ BinOps (| or &) side-by-side in ast; ast will break it down into a long tree of pairs of binops
    output_cond = re.sub("([^\w]\d*)or([^\w])",r"\1|\2",output_cond)
    output_cond = re.sub("([^\w]\d*)and([^\w])",r"\1&\2",output_cond)
    output_cond = re.sub("(^|[^\w]\d*)not([^\w])",r"\1~\2",output_cond)
    output_cond = unreplace_quotes(output_cond)

    # ast cannot handle unicode (\u) or hexcode (\x) characters. The characters are parsed as their code equivalent, and then here they are replaced with the actual character they represent (if the character is known), otherwise they are left as they're code equivalent
    for code in re.findall("\\\\\\\\u([\da-f]{4})",output_cond):
        output_cond = re.sub(re.escape("\\\\u" + code),eval("u'\\u" + code + "'"), output_cond)
    for code in re.findall("\\\\\\\\\\\\x([\da-f][\da-f])",output_cond):
        output_cond = re.sub(re.escape("\\\\\\x") + code,eval("u'\\u00" + code + "'"), output_cond)

    # Return converted string condition, and the data type the condition produces (bool, int, etc.)
    return output_cond, return_type


# ----------------------TREE TRAVERSAL------------------
node_to_real_predecessors = {}
node_to_real_successors = {}

def getRealPredecessors(node):
    '''Get predecessors of a node, skipping non-real nodes'''
    if not node or node.getID() in start_node_list:
        return []
    # If node is supernode, go inside supernode to get predecessors
    if node.getTypeName() in ["source_super","process_super","terminal_super"]:
        node = node.getChildDiagram().getOutputConnector()
    # If node is first node in supernode, go up a level and get supernodes predecessor
    elif node.getTypeName() == "input_connector":
        node = node.getOwner()
    if not node:
        return []
    stream = node.getProcessorDiagram()
    predecessors = list(stream.predecessors(node))
    # If node is a merge or append node, this organizes the predecessors based on which predecessor appears first in the node
    if len(predecessors) > 1:
        tags = [tag for tag in node.getProperty("tagTable").tagsInUse()]
        # If the node is a merge node doing a Partial Outer join, the predecessor that is keeping fields should be first in the order
        if node.getTypeName() == "merge" and str(node.getPropertyValue("join")) == "PartialOuter":
            tag_order = [str(tag) for tag in node.getFilterTableGroup().tagsInUse() if str(tag)]
            partial_dfs = [str(tag) for tag in tags if node.getKeyedPropertyValue("outer_join_tag",str(tag)) and str(tag) in tag_order]
            for tag in partial_dfs:
                tags.insert(0, tags.pop(tag_order.index(tag)))
        predecessors = [node.getPredecessor(tag) for tag in tags]
    real_predecessors = []
    for predecessor in predecessors:
        # If node isn't real, skip it and recursively get its predecessors
        if not isRealNode(predecessor) and predecessor.getID() not in start_node_list:
            real_predecessors += getRealPredecessors(predecessor)
        else:
            real_predecessors.append(predecessor)
    return real_predecessors
def getAvailableName(prefix,name_depth):
    '''Generates variable name not currently in use at the given depth
    ARGS:
        prefix: str - prefix the name should have eg. df_1
        name_depth: int - how many levels deep the name is. This determines whether to add an integer, letter, or roman numeral to the end. eg. df_2_a is two levels deep, so a name_depth=3 would yield df_2_a_i
    '''
    # Whether to index by number, letter, or roman numeral. 
    if name_depth %3 == 0:
        index_chars = range(676)
    elif name_depth %3 == 1:
        index_chars = letters
    else:
        index_chars = roman_numerals
    if name_depth > 0:
        prefix += "_"
    else:
        index_chars[0] = ""
    i = 0
    # increment index until name is found that is not currently in use somewhere in the code
    while prefix + str(index_chars[i]) in names_in_use:
        i+=1
    name = prefix + str(index_chars[i])
    return name

def orderBranch(node):
    '''Returns ordered list of all nodes before this one
    eg. Suppose a node diagram looked like this, with node E being a merge node, and node E was passed in:
    A -- C --\\
              E --- F
    B -- D --//
    The output would be: [A,C,B,D,E]
    '''
    predecessors = getRealPredecessors(node)

    # Populates map mapping nodes predecessors back to the node itself
    node_to_real_predecessors[node] = predecessors
    for predecessor in predecessors:
        if predecessor not in node_to_real_successors:
            node_to_real_successors[predecessor] = [node]
        else:
            node_to_real_successors[predecessor].append(node)

    node_order = []
    # Recursively orders the branch of each predecessor, appending each list one after another in one big list
    for predecessor in predecessors:
        # If predecessor has already been ordered by another branch, don't order it again, as this would lead to duplicate code
        if predecessor not in ordered_nodes:
            node_order += orderBranch(predecessor)

    # Supplies list of downstream nodes in the start_node_list, as well as those without any predecessors, to be used for future reference
    if node.getID() in start_node_list and node not in input_nodes:
        input_nodes.append(node)
    if not predecessors:
        source_nodes.append(node)

    # After adding predecessors to list, add node itself at the end
    if isRealNode(node) or node.getID() in end_node_list:
        node_order.append(node)
    ordered_nodes.add(node)

    return node_order

def orderBranchAll(node):
    '''A truncated version of orderBranch used only to identify the nodes downstream from the given node, without keeping track of the extra metadata orderBranch does'''
    node_order = []
    if node.getID() in start_node_list and node not in input_nodes:
        input_nodes.append(node)
    elif node.getID() not in start_node_list:
        stream = node.getProcessorDiagram()
        predecessors = stream.predecessors(node)

        for predecessor in predecessors:
            if predecessor not in ordered_nodes:
                node_order += orderBranchAll(predecessor)

        node_order.append(node)
        ordered_nodes.add(node)

    return node_order

# ------------------NODE CONVERSION -----------------
def cast(vals):
    '''This adds quotes to certain string values
    SPSS accepts values without quotes, and infers the type of the value based on its content. Python needs quotes to distinguish string values, so the same logic must be applied to see if a given value needs quotes or not
    ARGS:
        vals: str or List[str]
    eg.
        vals = 1 -> 1
        vals = a -> 'a'
        vals = 'a' -> 'a'
        vals = Jason's -> "Jason's"
        vals = [1,a,'a',Jason's] -> [1,'a','a',"Jason's"]
    '''

    if str(type(vals)) == "<type 'java.util.LinkedList'>":
        vals = list(vals)
    elif type(vals) != list:
        vals = [vals]

    new_vals = []
    for val in vals:
        val = str(val)
        # Don't add quotes if value already has quotes or is a digit
        if (val != "" and val[0] in ["'",'"'] and val[-1] == val[0] and not re.search(r'[^\\](?:\\\\)*' + val[0],val[:-1])) or re.search("^\d+$",val):
            new_vals.append(val)
        # Otherwise add single quotes if the value doesn't contain single quotes within it
        elif not re.search(r"(?:[^\\]|^)(?:\\\\)*'",val):
            new_vals.append("'" + val + "'")
        # If it does contain single quotes, try double quotes instead
        elif not re.search(r'(?:[^\\]|^)(?:\\\\)*"',val):
            new_vals.append('"' + val + '"')
        # If it contains both single and double quotes, use triple quotes
        else:
            new_vals.append("'''" + val + "'''")
    
    if len(new_vals) == 1:
        return new_vals[0]
    else:
        return new_vals
    

def convert_node(node):
    '''Given node, returns block of pyspark code as a string that does the same thing as the node'''
    if not isRealNode(node):
        return ""
    stream = node.getProcessorDiagram()
    # Gets python variable name assigned to this node
    var_name = node_to_name[node.getID()]
    # List of all python variable names of predecessors
    input_vars = [node_to_name[predecessor.getID()] for predecessor in node_to_real_predecessors[node]]
    # Python variable name of first predecessor
    if input_vars:
        prev_var_name = input_vars[0]
    else:
        prev_var_name = ""
    # All field names at the point of this node
    input_cols = [str(col) for col in node.getInputDataModel()]
    string_conversion = ""
    # ---- Filter ----
    # Filter node is done at the bottom, as merge nodes also use the filter node functionality
    if node.getTypeName() == "filter":
        pass
    # Unused nodes
    elif node.getTypeName() in ["type","table", "reorder"]:
        pass
    # ---- Merge ----
    elif node.getTypeName() == "merge":
        tags = list(node.getFilterTableGroup().tagsInUse())
        merge_method = node.getPropertyValue("method")
        keys = [str(key) for key in node.getPropertyValue("key_fields")]

        # Join Type (Order,Inner,FullOuter,PartialOuter,Anti)
        spss_join = str(node.getPropertyValue("join"))
        if merge_method == "Order":
            python_join = "inner"
        elif spss_join == "Inner":
            python_join = "inner"
        elif spss_join == "FullOuter":
            python_join = "outer"
        elif spss_join == "PartialOuter":
            tag_order = [str(tag) for tag in node.getFilterTableGroup().tagsInUse()]
            # List of checked predecessors in partial outer join
            partial_dfs = [tag for tag in tags if node.getKeyedPropertyValue("outer_join_tag",str(tag)) and str(tag) in tag_order]
            # SPSS allows a PartialOuter join where no predecessor is checked. In this case, it behaves like an inner join
            if len(partial_dfs) == 0:
                python_join = "inner"
            # Partial Outer join where multiple predecessors are checked has not been implemented
            elif len(partial_dfs) > 1:
                raise Exception("Not implemented (merge - PartialOuter)")
            else:
                # Move the dataframe associated with the checked predecessor to the front of the list, then left join all of the dataframes
                i = tag_order.index(partial_dfs[0])
                if merge_method == "Keys" and node.getPropertyValue("common_keys"):
                    tags.insert(1, tags.pop(i))
                else:
                    tags.insert(0, tags.pop(i))
                python_join = "left"
        elif spss_join == "Anti":
            python_join = "left_anti"

        
        to_drop = []

        # Join By (Keys,Condition,Order)
        if merge_method == "Keys" and node.getPropertyValue("common_keys"):
            param2 = "on=" + str(keys)
        # In the case 'Combine duplicate key fields' is unchecked. Each predecessors key fields must not be combined. This is done by joining by a condition instead of by keys
        elif merge_method == "Keys" and not node.getPropertyValue("common_keys"):
            param2 = "(" + ") & (".join([" == ".join([input_var + "['" + key + "']" for input_var in input_vars]) for key in keys]) + ")"
        elif merge_method == "Condition":
            # Puts condition into python form
            param2 = replace_calls(node.getPropertyValue("condition"), cast_bool=True, fields = input_cols)[0]
        # Merge by row number
        elif merge_method == "Order":
            for i in range(len(input_vars)):
                input_var = input_vars[i]
                new_input_var = input_var + "_MERGE"
                input_vars[i] = new_input_var
                string_conversion += new_input_var + " = " + input_var + ".withColumn('__ROW_NUMBER__', monotonically_increasing_id())\n"
                param2 = "on='__ROW_NUMBER__'"
            to_drop.append("__ROW_NUMBER__")
        elif merge_method == "Gtt":
            raise Exception("Not implemented (merge - Gtt)")
        
        # List of fields after the merge
        all_fields = []
        # Duplicate field names that exist in more than one predecessor
        duplicate_columns = set()
        # Determine if two predecessors have a field with the same name, creating a conflict in the merged dataframe
        if spss_join != "Anti":
            for predecessor in node_to_real_predecessors[node]:
                for col_name in predecessor.getOutputFieldSet():
                    col_name = str(col_name)
                    # Keys don't count as duplicates if they're being merged together
                    if not (merge_method == "Keys" and node.getPropertyValue("common_keys")) or col_name not in keys:
                        if col_name in all_fields:
                            duplicate_columns.add(col_name)
                        all_fields.append(col_name)
            if merge_method == "Keys" and node.getPropertyValue("common_keys"):
                all_fields += keys

        # Add corresponding tag to start of duplicate column name if option 'Add tags to duplicate field names to avoid merge conflicts' is enabled
        if merge_method == "Condition" and node.getPropertyValue("rename_duplicate_fields"):
            for i in range(len(input_vars)):
                input_var = input_vars[i]
                rename_keys = []
                rename_vals = []
                for col_name in node_to_real_predecessors[node][i].getOutputFieldSet():
                    col_name = str(col_name)
                    if col_name in duplicate_columns:
                        rename_keys.append(col_name)
                        rename_vals.append(str(tags[i]) + "_" + col_name)
                if rename_keys:
                    new_input_var = input_var
                    if new_input_var[-6:] != "_MERGE":
                        new_input_var += "_MERGE"
                    string_conversion += '''%s = conversion_helper.rename_columns(%s,%s,%s)\n'''%(new_input_var,input_var,str(rename_keys),str(rename_vals))
                    input_vars[i] = new_input_var
            duplicate_columns = set()
        
        # Dropped/Renamed Columns
        to_keep = []
        rename_old_names = []
        rename_new_names = []
        for i in range(len(tags)):
            tag = tags[i]
            filterTable = node.getFilterTableGroup().getFilterTable(tag)
            if not filterTable is None:
                for col_name in filterTable.inputFieldSet:
                    col_name = str(col_name)
                    # Must add `` quotes if column name has a . in it for pyspark to understand it
                    if "." in col_name:
                        quoted_col_name = "`" + col_name + "`"
                    else:
                        quoted_col_name = col_name
                    if not duplicate_columns or (col_name in keys and node.getPropertyValue("common_keys")):
                        col_name_alias = quoted_col_name
                    # If duplicate columns, add predecessor variable name to front of field name to distinguish it from other predecessors eg. df.TRAVEL_APP_NO
                    elif duplicate_columns and col_name not in keys and node.getPropertyValue("common_keys"):
                        col_name_alias = input_vars[i-1] + "." + quoted_col_name
                    else:
                        col_name_alias = input_vars[i] + "." + quoted_col_name
                    # Gets list of columns dropped after merge node
                    if filterTable.getDropped(col_name):
                        to_drop.append(col_name_alias.replace("`",""))
                    # Gets lists of column names renamed after merge node
                    else:
                        to_keep.append(col_name_alias)
                        if col_name != filterTable.getNewName(col_name):
                            rename_old_names.append(col_name_alias)
                            rename_new_names.append(str(filterTable.getNewName(col_name)))
            # Populates a list of the columns that are not dropped by the merge node
            elif merge_method == "Keys" and node.getPropertyValue("common_keys"):
                to_keep += ["`" + str(col_name) + "`" if "." in str(col_name) else str(col_name) for col_name in node_to_real_predecessors[node][i-1].getOutputFieldSet()]
            else:
                to_keep += ["`" + str(col_name) + "`" if "." in str(col_name) else str(col_name) for col_name in node_to_real_predecessors[node][i].getOutputFieldSet()]
        
        # If duplicate field names, the predecessors are aliased with their variable name. This allows their fields to be distinguished from one another by preceding the field name with its variable name eg. df.TRAVEL_APP_NO
        if duplicate_columns:
            for input_var in input_vars:
                string_conversion += "%s = %s.alias('%s')\n"%(input_var,input_var,input_var)
            merge_name = var_name + "_MERGE"
        else:
            merge_name = var_name
        
        # Join condition. Each predecessor is joined with the predecessor before it one after the other with the order dictated by input_vars
        if len(input_vars) > 0:
            string_conversion += "%s = %s"%(merge_name,input_vars[0])
            for input_var in input_vars[1:]:
                string_conversion += ".join(%s,%s,how=\"%s\")\\\n    "%(input_var,str(param2),python_join)
            string_conversion = string_conversion[:-6] + "\n"

        # Filter/Rename: if duplicate columns, have to use special method rename_and_filter_postmerge to be able to drop and rename them correctly
        if duplicate_columns:
            input_vars_no_str = "["
            for input_var in input_vars:
                input_vars_no_str+= input_var + ", "
            input_vars_no_str += "]"
            if not node.getPropertyValue("common_keys"):
                keys = []
            # Supplies rename_and_filter_postmerge method with merged dataframe, keys, the predecessor dataframes, and the predecessor dataframe aliases
            string_conversion += '''%s = rename_and_filter_postmerge(%s, %s, %s, %s, '''%(var_name,merge_name,str(keys),str(input_vars_no_str),str(input_vars))
            # Supplies rename_and_filter_postmerge method with fieldnames to drop and to rename
            if to_drop:
                if len(all_fields) // 2 < len(to_keep):
                    string_conversion += "to_drop = " + str(to_drop) + ", "
                else:
                    string_conversion += "to_select = " + str(to_keep) + ", "
                prev_var_name = var_name
            if rename_old_names:
                string_conversion += "to_rename = " + str(rename_old_names) + ", renamed = " + str(rename_new_names)
            string_conversion += ")\n"
        # Drop and rename columns
        else:
            if to_drop:
                if len(all_fields) // 2 < len(to_keep):
                    string_conversion += '''to_drop = %s\n%s = %s.drop(*to_drop)\n'''%(str(to_drop),var_name,var_name)
                # If less kept columns then dropped columns, cleaner to just select the kept columns instead of dropping all of the dropped columns
                else:
                    string_conversion += "%s = %s.select(%s)\n"%(var_name, var_name, str(to_keep)[1:-1])
            if rename_old_names:
                string_conversion += '''%s = conversion_helper.rename_columns(%s,%s,%s)\n'''%(var_name,var_name,str(rename_old_names),str(rename_new_names))
    # ---- User Input ----
    elif node.getTypeName() == "userinput":
        columns = [str(col) for col in node.getPropertyValue("names")]
        input_lists = []
        max_length = 0
        product_arr = []

        # Iterate through columns and parse the list of data into python
        for i in range(len(columns),0,-1):
            col_name = columns[i-1]
            data = "[" + str(node.getKeyedPropertyValue("data",col_name)) + "]"
            data = replace_quotes_and_comments(data)
            data = spss_lists_to_python_lists(data)
            data = unreplace_quotes(data)
            data = eval(data)
            if not product_arr:
                product_arr = [1]
            else:
                product_arr.insert(0,product_arr[0]*len(input_lists[0]))
            if not data:
                data = [None]
            # Max length is needed to determine the size of the resulting dataframe if 'Ordered' is selected
            max_length = max(max_length,len(data))
            input_lists.insert(0,data)
        # If 'Ordered' is selected, transposes data so that row data is together, not column data
        if node.getPropertyValue("data_mode") == "Ordered":
            values = [[] for i in range(max_length)]
            for input_list in input_lists:
                for i in range(len(input_list)):
                    values[i].append(input_list[i])
                for i in range(len(input_list),max_length):
                    values[i].append(None)
        # Otherwise, explodes data so that every combination of each value in the columns exists as a row in the new dataframe
        else:
            transposed_values = []
            for i in range(len(input_lists)):
                input_list = input_lists[i]
                new_list = []
                for j in range(len(input_list)):
                    val = input_list[j]
                    new_list += [val]*product_arr[i]
                new_list *= len(input_lists[0])*product_arr[0]/len(new_list)
                transposed_values.append(new_list)
            values = [[] for i in range(len(transposed_values[0]))]
            for i in range(len(transposed_values)):
                for j in range(len(transposed_values[i])):
                    values[j].append(transposed_values[i][j])

        values = "[" + str(values)[1:-1].replace("[","\n\t[") + "\n]"
        col_str = ""
        # Iterate through columns, mapping each column to its type
        for col_name in columns:
            old_type = str(node.getKeyedPropertyValue("custom_storage",col_name))
            type_map = {
                "String":"string",
                "Integer":"integer",
                "Real":"float",
                "Date":"date"
            }
            if old_type not in type_map:
                print(str(node) + ": overriding to type " + old_type + " is not yet supported")
            # Specifies the column type as defined in the user input node so that the data is parsed the same
            col_str += col_name + ":" + type_map[old_type] + ", "
        # Create the equivalent pyspark dataframe
        string_conversion += '''%s = spark.createDataFrame(%s,'%s')\n'''%(var_name,values,col_str)
    # ---- Sort ----
    elif node.getTypeName() == "sort":
        # List of sorted columns, and whether they're Ascending or Descending
        sort_info = [(str(col_name), str(direction)) for col_name, direction in node.getPropertyValue("keys") if str(col_name) in input_cols]
        string_conversion += var_name + " = " + prev_var_name + ".sort(\n"
        for col_name, direction in sort_info:
            if "." in col_name:
                quoted_col_name = "`" + col_name + "`"
            else:
                quoted_col_name = col_name
            # Pyspark sorts string data ignoring case. This is mimicked by python by converting the data to lowercase while sorting
            if str(node.getInputDataModel().getStorageType(col_name)) == "String":
                string_conversion += "\tlower(col(\"" + quoted_col_name + "\"))"
            else:
                string_conversion += "\tcol(\"" + quoted_col_name + "\")"
            # Adds .desc() suffix to sort the column descending. Default is ascending
            if direction == "Descending":
                string_conversion += ".desc()"
            string_conversion += ",\n"
        string_conversion += ")\n"
    # ---- Aggregate ----
    elif node.getTypeName() == "aggregate":
        # Name of record count columns
        record_count_col = str(node.getPropertyValue("count_field"))
        keys = [str(key) for key in node.getPropertyValue("keys") if str(key) in input_cols]
        quoted_keys = ["`" + key + "`" if "." in key else key for key in keys]
        # List of fields being aggregated
        aggregates = [str(agg_col)for agg_col in node.getKeyedPropertyKeys("aggregates") if str(agg_col) in input_cols and str(agg_col) != record_count_col]
        # Extension to add to aggregated field names
        extension = str(node.getPropertyValue("extension"))
        # Whether to add extension as a suffix or a prefix to the field name
        add_as = str(node.getPropertyValue("add_as"))
        # Creates a column recording the row order before aggregating. The .groupBy method loses track of the key order, so by later sorting by this field, the key order is retained
        string_conversion += var_name + " = " + prev_var_name + ".withColumn('__ROW_NUMBER__',row_number().over(Window.orderBy(monotonically_increasing_id())))\n"
        if aggregates or node.getPropertyValue("inc_record_count"):
            # Maps SPSS aggregate method to the pyspark equivalent, as well as the field suffix to be added to the aggregated field name
            agg_map = {"Max":("_max","_Max"),"Min":("_min","_Min"),"Sum":("_sum","_Sum"),"Count":("_count","_Count"),"Mean":("mean","_Mean")}
            string_conversion += var_name + " = " + var_name + ".groupBy(" + str(quoted_keys) + ").agg("
            # Iterates through each column being aggregated
            for col in aggregates:
                aggs = node.getKeyedPropertyValue("aggregates", col)
                # Iterates through each way the column is being aggregated
                for agg in aggs:
                    new_name = str(col) + agg_map[agg][1]
                    if extension and add_as == "Prefix":
                        new_name = extension + "_" + new_name
                    elif extension and add_as == "Suffix":
                        new_name = new_name + "_" + extension
                    # Aliases the aggregated column with a new name
                    string_conversion += "\n\t" + agg_map[agg][0] + "(\"" + str(col) + "\").alias(\"" + new_name + "\"),"
            # Record Count field is supplied by the 'count' pyspark method
            if node.getPropertyValue("inc_record_count"):
                string_conversion += "\n\t_count(lit(1)).alias(\"" + record_count_col + "\"),"
            # Gets smallest row number of group
            string_conversion += "\n\tfirst('__ROW_NUMBER__').alias('__ROW_NUMBER__'),"
            string_conversion += "\n)"
        # If all fields are selected as key fields, duplicate rows are dropped
        elif set([str(col) for col in node.getInputDataModel()]) == set(keys):
            string_conversion += var_name + " = " + var_name + ".drop_duplicates(" + var_name + ".columns[:-1])"
        # If no aggregates, only the key fields are selected, and duplicates are dropped
        else:
            string_conversion += var_name + " = " + var_name + ".select(" + str(quoted_keys)[1:-1] + ",'__ROW_NUMBER__')\n"
            string_conversion += var_name + " = " + var_name + ".drop_duplicates(" + str(keys) + ")"
        # Sorts the dataframe the way is was before
        string_conversion += ".sort('__ROW_NUMBER__').drop('__ROW_NUMBER__')\n"
    # ---- Distinct ----
    elif node.getTypeName() == "distinct":
        keys = [str(key) for key in node.getPropertyValue("grouping_fields") if str(key) in input_cols]
        quoted_keys = ["`" + key + "`" if "." in key else key for key in keys]

        # Captures row order in column so that row order can be retained
        string_conversion += var_name + " = " + prev_var_name + ".withColumn('__ROW_NUMBER__',row_number().over(Window.orderBy(monotonically_increasing_id())))\n"

        # Sorts fields if specified in distinct node
        sort_info = [(str(col_name), str(direction)) for col_name, direction in node.getPropertyValue("existing_sort_keys") if str(col_name) in input_cols]
        if sort_info:
            sort_string = "\n"
            for col_name, direction in sort_info:
                if "." in col_name:
                    col_name = "`" + col_name + "`"
                if str(node.getInputDataModel().getStorageType(col_name)) == "String":
                    sort_string += "\tlower(col(\"" + col_name + "\"))"
                else:
                    sort_string += "\tcol(\"" + col_name + "\")"
                if direction == "Descending":
                    sort_string += ".desc()"
                sort_string += ",\n"
        else:
            sort_string = "monotonically_increasing_id()"

        # Include only first record from each group
        if node.getPropertyValue("mode") == "Include":
            # Numbers rows in each key group, then selects the first one
            string_conversion += '''window = Window.partitionBy(%s).orderBy(%s)
%s = %s.withColumn("__ROW_NUMBER_GROUP__",row_number().over(window)).filter(col("__ROW_NUMBER_GROUP__") == 1).sort('__ROW_NUMBER__').drop("__ROW_NUMBER__","__ROW_NUMBER_GROUP__")
'''%(str(quoted_keys), sort_string, var_name, var_name)
        # Create a composite record for each group
        elif node.getPropertyValue("mode") == "Composite":
            # Warning: due to a bug, SPSS distinct nodes do not properly sort fields when 'Composite' is selected
            if sort_info:
                print(str(node) + ": Composite Distinct nodes cannot properly sort within the node due to an SPSS bug. Delete the sort fields. If you still want to sort, put a sort node before the distinct node")
                string_conversion += var_name + " = " + var_name + ".sort(" + sort_string + ")\n"
            
            record_count_col = str(node.getPropertyValue("count_field"))

            # List of column names and their aggregates in the distinct node
            tuples = [(col, agg) for col, agg in node.getPropertyValue("composite_values") if str(col) in input_cols and str(col) not in keys and str(col) != record_count_col]

            # Maps SPSS delimiter label with the character itself
            delim_map = {"":"","Space":" ","Comma":",","UnderScore(_)":"_"}
            # List of columns being concatenated, and the delimiter that is used to join the values
            concat_cols = [(col_name,agg[0][1]) for col_name,agg in tuples if str(type(agg[0])) == "<type 'java.util.Arrays$ArrayList'>" and agg[0][0] == "Concatenate"]
            # Pyspark does not have a concatenation aggregate, so concatenation is done first before the aggregation using a Window
            if concat_cols:
                string_conversion += "window = Window.partitionBy(" + str(quoted_keys) + ").orderBy(monotonically_increasing_id())\n"
                # Iterates through each column that is to be concatenated
                for col_name,delim in concat_cols:
                    col_name = str(col_name)
                    if "." in col_name:
                        col_name = "`" + col_name + "`"
                    # Collects group into a list
                    string_conversion += var_name + " = " + var_name + ".withColumn('" + col_name + "',array_distinct(collect_list('" + col_name + "').over(window)))\n"
                    # Joins list values by the delimiter
                    string_conversion += var_name + " = " + var_name + ".withColumn('" + col_name + "',when(col('" + col_name + "') != array(),array_join(col('" + col_name + "'), '" + delim_map[delim] + "')).otherwise(lit(None)))\n"

            # Map of SPSS aggregates to equivalent pyspark aggregates Note: by aggregating the concatenated columns by _max, it ensures that the largest value in the group is taken, containing all of the values in the group
            agg_map = {"Max":"_max","Min":"_min","First":"first","Last":"last","Total":"_sum","FirstAlpha":"_min","LastAlpha":"_max","Latest":"_max","Earliest":"_min","Concatenate":"_max"}
            string_conversion += var_name + " = " + var_name + ".groupBy(" + str(quoted_keys) + ").agg("
            for col, agg in tuples:
                agg = agg[0]
                # Concatenate aggregate is a tuple also containing the delimiter. Only the aggregate is used for this part
                if str(type(agg)) == "<type 'java.util.Arrays$ArrayList'>" and agg[0] == "Concatenate":
                    agg = agg[0]
                if agg in agg_map:
                    string_conversion += "\n\t" + agg_map[agg] + "(\"" + str(col) + "\").alias(\"" + str(col) + "\"),"
                else:
                    raise Exception("Not implemented (Distinct - Composite - Undefined Aggregate: " + str(agg) + ")")
            if node.getPropertyValue("inc_record_count"):
                string_conversion += "\n\t_count('*').alias(\"" + str(record_count_col) + "\"),"
            string_conversion += "\n\tfirst('__ROW_NUMBER__').alias('__ROW_NUMBER__'),"
            # Sorts row order to have it was before the node
            string_conversion += "\n).sort('__ROW_NUMBER__').drop('__ROW_NUMBER__')\n"
        else:
            raise Exception("Not implemented (Distinct)")
    # ---- Database ----
    elif node.getTypeName() == "database":
        tablename = str(node.getPropertyValue("tablename")).strip()
        
        # If tablename begins with '_OPENED', creates switch statement triggered from config file between it and its dev equivalent ending in '_DEV'
        if re.search("_OPENED$",tablename):
            dev_tablename = re.sub("_OPENED$","_DEV",tablename)
            string_conversion += '''
if parent_config_file['prod']:
    %s = read_file(spark, parent_config_file["views_directory"] + "%s.parquet", trim_spaces = parent_config_file['trim_inputs'])
else:
    %s = read_file(spark, parent_config_file["views_directory"] + "%s.parquet", trim_spaces = parent_config_file['trim_inputs'])
'''[1:]%(var_name,tablename,var_name,dev_tablename)
        # Assumes psypark equivalent of table is a parquet file of the same name
        else:
            string_conversion += var_name + " = read_file(spark, parent_config_file[\"views_directory\"] + \"" + tablename + ".parquet\", trim_spaces = parent_config_file['trim_inputs'])\n"
        prev_var_name = var_name
    # ---- Select ----
    elif node.getTypeName() == "select":
        # Convert condition to python
        cond = replace_calls(node.getPropertyValue("condition"),cast_bool=True,fields=input_cols)[0]
        # If Discard is enabled, simply add not(~) symbol to the front of the condition
        if node.getPropertyValue("mode") == "Discard":
            cond = "~(" + cond + ")"
        string_conversion += var_name + " = " + prev_var_name + ".filter(" + cond + ")\n"
    # ---- Derive ----
    elif node.getTypeName() == "derive":
        # If multiple columns, the same logic as if it were a single column is used, just in a for loop over all the columns
        if node.getPropertyValue("mode") == "Multiple":
            fields = [str(field) for field in node.getPropertyValue("fields") if str(field) in input_cols]
            string_conversion += "cols = " + str(fields) + "\n"
            if prev_var_name != var_name:
                string_conversion += var_name + " = " + prev_var_name + "\n"
                prev_var_name = var_name
            string_conversion += '''for col_name in cols:\n\t'''
            # New field name has an extension added either as a prefix or a suffix
            if node.getPropertyValue("add_as") == "Suffix":
                new_name = "col_name + '%s'"%(node.getPropertyValue("name_extension"))
            else:
                new_name = "'%s' + col_name"%(node.getPropertyValue("name_extension"))
        else:
            # If single derive column, new field name is received from derive node
            new_name = "\"" + node.getPropertyValue("new_name") + "\""
        
        # Derive as: Flag
        if node.getPropertyValue("result_type") == "Flag":
            cond = replace_calls(node.getPropertyValue("flag_expr"),"col_name",cast_bool=True,fields=input_cols)[0]
            # Adds quotes to true/false value if they don't already have them
            true_val, false_val = cast([node.getPropertyValue("flag_true").strip(),node.getPropertyValue("flag_false").strip()])
            string_conversion += var_name + " = " + prev_var_name + ".withColumn(" + new_name + ", when(" + cond + ", " + true_val + ").otherwise(" + false_val + "))\n"
        # Derive as: Formula
        elif node.getPropertyValue("result_type") == "Formula":
            cond = replace_calls(node.getPropertyValue("formula_expr"),"col_name",fields=input_cols)[0]
            string_conversion += var_name + " = " + prev_var_name + ".withColumn(" + new_name + ", " + cond + ")\n"
        # Derive as: Nominal
        elif node.getPropertyValue("result_type") == "Set":
            text = var_name + " = " + prev_var_name + ".withColumn(" + new_name + ", \n\t"

            derive_table = node.getDeriveSetTable()
            # List of nominal values, and their conditions
            val_conds = []
            for i in range(derive_table.getRowCount()):
                val_conds.append((derive_table.getName(i),derive_table.getCondition(i)))

            # Otherwise value
            otherwise_val = cast(node.getPropertyValue("set_default").strip())
            # Iterates through nominal values
            for val,cond in val_conds:
                cond = replace_calls(cond,"col_name",cast_bool=True,fields=input_cols)[0]
                val = cast(val.strip())
                # Adds condition as a when statement to the previous 'withColumn'
                text += "when(\n\t\t" + tabify(cond + ", " + val).replace("\n","\n\t\t") + "\n\t)."
            text += "otherwise(" + otherwise_val + ")\n)"
            if node.getPropertyValue("mode") == "Multiple":
                text = text.replace("\n","\n\t") 
            string_conversion += text + "\n"
        # Derive as: Count
        # This behaviour is difficult to replicate in pyspark. Instead, the code recognizes a series of common cases, and supplies pyspark code that does the same thing as the case
        elif node.getPropertyValue("result_type") == "Count":
            # Increment condition
            inc_cond = re.search("^ (\w+) /= undef $".replace(" ","\s*"), node.getPropertyValue("count_inc_condition"))
            inc_cond2 = re.search("^ @OFFSET\( (\w+) , 1 \) /= (\w+) $".replace(" ","\s*"), node.getPropertyValue("count_inc_condition"))
            # Reset condition
            reset_cond = re.search("^ @OFFSET\( (\w+) , 1 \) /= (\w+) $".replace(" ","\s*"), node.getPropertyValue("count_reset_condition"))
            # Case 1: Initial value = 0, increment by 1, increment condition = '<field> /= undef', reset condition = '@OFFSET(<field>, 1) /= <field>'
            if (
                node.getPropertyValue("count_initial_val") == 0 
                and str(node.getPropertyValue("count_inc_expression")) == "1" 
                and inc_cond 
                and reset_cond
                and inc_cond.group(1) == reset_cond.group(1)
                and reset_cond.group(1) == reset_cond.group(2)
            ):
                rank_col = reset_cond.group(1)
                if "." in rank_col:
                    rank_col = "`" + rank_col + "`"
                text = '''windowAppNum = Window.partitionBy(["%s"]).orderBy(monotonically_increasing_id())
%s = %s.withColumn(%s,row_number().over(windowAppNum))\
'''%(rank_col,var_name,prev_var_name,new_name)
                if node.getPropertyValue("mode") == "Multiple":
                    text = text.replace("\n","\n\t")
                string_conversion += text + "\n"
            # Case 2: Same as Case 1, except increment condition != '<field> /= undef'
            elif (
                node.getPropertyValue("count_initial_val") == 0 
                and str(node.getPropertyValue("count_inc_expression")) == "1" 
                and reset_cond
                and reset_cond.group(1) == reset_cond.group(2)
            ):
                rank_col = reset_cond.group(1)
                if "." in rank_col:
                    rank_col = "`" + rank_col + "`"
                new_cond = replace_calls(node.getPropertyValue("count_inc_condition"),"col_name",fields=input_cols)[0]
                text = '''%s = %s.withColumn("__TEMP_FLAG__",when(%s,1).otherwise(0))
windowval = (Window.partitionBy('%s').orderBy('%s').rowsBetween(Window.unboundedPreceding, 0))
%s = %s.withColumn(%s,_sum("__TEMP_FLAG__").over(windowval)).drop("__TEMP_FLAG__")
'''.strip()%(var_name,prev_var_name,new_cond,rank_col,rank_col,var_name,var_name,new_name)
                if node.getPropertyValue("mode") == "Multiple":
                    text = text.replace("\n","\n\t")
                string_conversion += text + "\n"
            # Case 3: Same as Case 1, except reset condition != '@OFFSET(<field>, 1) /= <field>'
            elif (
                node.getPropertyValue("count_initial_val") == 0 
                and str(node.getPropertyValue("count_inc_expression")) == "1" 
                and inc_cond
                and re.search("^\s*$",node.getPropertyValue("count_reset_condition"))
            ):
                rank_col = inc_cond.group(1)
                new_cond = replace_calls(node.getPropertyValue("count_inc_condition"),"col_name",fields=input_cols)[0]
                text = '''%s = %s.withColumn("__TEMP_FLAG__",when(%s,1).otherwise(0))
%s = %s.withColumn(%s,_sum("__TEMP_FLAG__").over(Window.orderBy(monotonically_increasing_id()))).drop("__TEMP_FLAG__")
'''.strip()%(var_name,prev_var_name,new_cond,var_name,var_name,new_name)
                if node.getPropertyValue("mode") == "Multiple":
                    text = text.replace("\n","\n\t")
                string_conversion += text + "\n"
            elif (
                inc_cond2
                and node.getPropertyValue("count_initial_val") == 0 
                and str(node.getPropertyValue("count_inc_expression")) == "1" 
                and re.search("^\s*1\s*=\s*0\s*$",node.getPropertyValue("count_reset_condition"))
            ):
                inc_col = inc_cond2.group(1)
                if "." in inc_col:
                    inc_col = "`" + inc_col + "`"
                text = "%s = %s.withColumn(%s, dense_rank().over(Window.partitionBy().orderBy('%s'))-1)"%(var_name,prev_var_name,new_name,inc_col)
                if node.getPropertyValue("mode") == "Multiple":
                    text = text.replace("\n","\n\t")
                string_conversion += text + "\n"
            else:
                raise Exception("Derive - Count not implemented")
        # Derive as: Conditional
        # Simply puts the if/then/else fields into a when expression
        elif node.getPropertyValue("result_type") == "Conditional":
            if_cond = replace_calls(node.getPropertyValue("cond_if_cond"),"col_name",cast_bool=True,fields=input_cols)[0]
            then_expr, then_type = replace_calls(node.getPropertyValue("cond_then_expr"),"col_name",fields=input_cols)
            else_expr, else_type = replace_calls(node.getPropertyValue("cond_else_expr"),"col_name",fields=input_cols)
            string_conversion += var_name + " = " + prev_var_name + ".withColumn(" + new_name + ", when(" + if_cond + ", " + then_expr + ").otherwise(" + else_expr + "))\n"
        else:
            raise Exception("Not implemented (derive - " + node.getPropertyValue("result_type") + ")")
    # ---- Append ----
    elif node.getTypeName() == "append":
        if node.getPropertyValue("create_tag_field"):
            raise Exception("Not implemented (append - create tag)")
        if node.getPropertyValue("match_case"):
            raise Exception("Not implemented (append - match case)")
        if node.getPropertyValue("match_by") == "Position":
            raise Exception("Not implemented (append - match by position)")
        # Columns from all predecessors included
        if node.getPropertyValue("include_fields_from") == "All":
            # Logic to take union of each predecessors fields
            final_cols_str = "set(" + input_vars[0] + ".columns)"
            for input_var in input_vars[1:]:
                final_cols_str += ".union(set(" + input_var + ".columns))"
            final_cols = set()
            for predecessor in node_to_real_predecessors[node]:
                final_cols |= set([str(col_name) for col_name in predecessor.getOutputFieldSet()])
        # Only columns from main predecessor included
        else:
            final_cols_str = "set(" + input_vars[0] + ".columns)"
            # Only takes columns from main predecessor
            final_cols = set([str(col_name) for col_name in node_to_real_predecessors[node][0].getOutputFieldSet()])
        string_conversion += "final_cols = " + final_cols_str + "\n"
        # Adds/removes columns from each predecessor so that each predecessor has the same columns
        for input_var in input_vars:
            new_input_var = input_var + "_APPEND"
            is_input_var_main = input_var == input_vars[0]
            # Removes extra columns that main predecessor does not have
            if node.getPropertyValue("include_fields_from") != "All" and not is_input_var_main:
                string_conversion += new_input_var + " = " + input_var + ".drop(*(set(" + input_var + ".columns) - final_cols))\n"
                input_var = new_input_var
            # Adds empty columns that predecessor does not have 
            if node.getPropertyValue("include_fields_from") == "All" or not is_input_var_main:
                if input_var != new_input_var:
                    string_conversion += new_input_var + " = " + input_var + "\n"
                # Iterates through each missing column and adds it
                string_conversion += "for col_name in final_cols - set(" + input_var + ".columns):\n"
                string_conversion += "\t" + new_input_var + " = " + new_input_var + ".withColumn(col_name, lit(None))\n"
             
        # Uses multi_union function to append predecessors
        string_conversion += var_name + " = multi_union("
        # Adds each predecessor dataframe as argument in multi_union function
        for input_var in input_vars:
            if node.getPropertyValue("include_fields_from") == "All" or input_var != input_vars[0]:
                input_var += "_APPEND"
            string_conversion += input_var + ", "
        string_conversion += ")\n"
        
        output_cols = set([str(col_name) for col_name in node.getOutputFieldSet()])
        # Drops columns not in the append node after appending
        to_drop = [col_name for col_name in final_cols if str(col_name) not in output_cols]
        if to_drop:
            string_conversion += '''to_drop = %s\n%s = %s.drop(*to_drop)\n'''%(str(to_drop),var_name,var_name)
    # ---- Filler ----
    elif node.getTypeName() == "filler":
        fields = [str(field) for field in node.getPropertyValue("fields") if str(field) in input_cols]
        # If filling more than one field, uses for loop
        if len(fields) > 1:
            string_conversion += "cols = " + str(fields) + "\n"
            if prev_var_name != var_name:
                string_conversion += var_name + " = " + prev_var_name + "\n"
                prev_var_name = var_name
            string_conversion += '''for col_name in cols:\n\t'''
            field_name = "col_name"
        else:
            field_name = "'" + fields[0] + "'"

        replace_cond = node.getPropertyValue("replace_with")
        # Makes annotations much more efficient
        replace_cond = re.sub('''if length \\( trim \\( @FIELD \\) \\) > 0 then (@FIELD >< .*|.* >< @FIELD) else .* endif'''.replace(" ",r"\s*"),"\\1",replace_cond)
        # Converts replace expression to python
        replace_cond = replace_calls(replace_cond, field_name = field_name,fields=input_cols)[0]
        if "." in field_name:
            otherwise_val = "col('`" + field_name[1:-1] + "`')"
        else:
            otherwise_val = "col(" + field_name + ")"
        # Replace: Based on condition
        if node.getPropertyValue("replace_mode") == "Conditional":
            cond = node.getPropertyValue("condition")
            cond = replace_calls(cond, field_name = field_name,cast_bool=True,fields=input_cols)[0]
            string_conversion += var_name + " = " + prev_var_name + ".withColumn(" + field_name + ", when(" + cond + ", " + replace_cond + ").otherwise(" + otherwise_val + "))\n"
        # Replace: Always
        elif node.getPropertyValue("replace_mode") == "Always":
            string_conversion += var_name + " = " + prev_var_name + ".withColumn(" + field_name + ", " + replace_cond+ ")\n"
        # Replace: Blank values
        elif node.getPropertyValue("replace_mode") == "Blank":
            # Converts the equivalent SPSS expression '@BLANK(@FIELD)' to python
            cond = replace_calls("@BLANK(@FIELD)", field_name = field_name,cast_bool=True,fields=input_cols)[0]
            string_conversion += var_name + " = " + prev_var_name + ".withColumn(" + field_name + ", when(" + cond + ", " + replace_cond + ").otherwise(" + otherwise_val + "))\n"
        # Replace: Null values
        elif node.getPropertyValue("replace_mode") == "Null":
            # Converts the equivalent SPSS expression '@NULL(@FIELD)' to python
            cond = replace_calls("@NULL(@FIELD)", field_name = field_name,cast_bool=True,fields=input_cols)[0]
            string_conversion += var_name + " = " + prev_var_name + ".withColumn(" + field_name + ", when(" + cond + ", " + replace_cond + ").otherwise(" + otherwise_val + "))\n"
        # Replace: Blank and Null values
        elif node.getPropertyValue("replace_mode") == "BlankAndNull":
            # Converts the equivalent SPSS expression '@BLANK(@FIELD) or @NULL(@FIELD)' to python
            cond= replace_calls("@BLANK(@FIELD) or @NULL(@FIELD)", field_name = field_name,cast_bool=True,fields=input_cols)[0]
            string_conversion += var_name + " = " + prev_var_name + ".withColumn(" + field_name + ", when(" + cond + ", " + replace_cond + ").otherwise(" + otherwise_val + "))\n"
    # ---- Set To Flag ----
    elif node.getTypeName() == "settoflag":
        set_field = node.getKeyedPropertyKeys("fields_from")[0]
        cols = [str(col_name) for col_name in node.getKeyedPropertyValue("fields_from",set_field)]
        keys = [str(col_name) for col_name in node.getPropertyValue("keys") if str(col_name) in input_cols]
        quoted_keys = ["`" + key + "`" if "." in key else key for key in keys]
        true_val, false_val = cast([node.getPropertyValue("true_value").strip(),node.getPropertyValue("false_value").strip()])
        string_conversion += "vals = " + str(cols) + "\n"
        # Add prefix to new field name
        if node.getPropertyValue("use_extension") and node.getPropertyValue("add_as") == "Prefix":
            new_col_name = "'" + node.getPropertyValue("extension") + set_field + "_' + val"
        # Add suffix to new field name
        elif node.getPropertyValue("use_extension") and node.getPropertyValue("add_as") == "Suffix":
            new_col_name = "'" + set_field + node.getPropertyValue("extension") + "_' + val"
        else:
            new_col_name = "'" + set_field + "_' + val"
        if "." in set_field:
            set_field_quotes = "`" + set_field + "`"
        else:
            set_field_quotes = set_field
        new_col_name_quotes = "'`" + new_col_name[1:] + " + '`'"
        # Assigns new variable to node so that predecessor variable is not touched
        if prev_var_name != var_name:
            string_conversion += var_name + " = " + prev_var_name + "\n"
        # If Aggregate Keys:
        if keys:
            # Take max flag over keys
            string_conversion += '''\
windowAppNum = Window.partitionBy(%s)
for val in vals:
    %s = %s.withColumn(%s, when(col("%s") == val, 1).otherwise(0))
    %s = %s.withColumn(%s, _max(col(%s)).over(windowAppNum))
'''%(str(quoted_keys)[1:-1], var_name, var_name, new_col_name, set_field_quotes, var_name, var_name, new_col_name, new_col_name_quotes)
            # If the True/False value is anything other than 1 and 0, replace the 1's and 0's with the specified true/false values
            if true_val != "1" or false_val != "0":
                string_conversion += '''\t%s = %s.withColumn(%s, when(col(%s) == 1, %s).otherwise(%s))\n'''%(var_name, var_name, new_col_name, new_col_name_quotes, true_val, false_val)
            # Selects only keys and new flag columns
            string_conversion += '''%s = %s.drop_duplicates(%s).select(*[%s + [%s for val in vals]])\n'''%(var_name, var_name, keys, quoted_keys, new_col_name_quotes)
        else:
            # For each specified value, creates new column with the true value on rows where that value exists, and the false value otherwise
            string_conversion += '''\
for val in vals:
    %s = %s.withColumn(%s, when(col("%s") == val, %s).otherwise(%s))
'''%(var_name, var_name, new_col_name, set_field_quotes, true_val, false_val)
    # ---- Reclassify ----
    elif node.getTypeName() == "reclassify":
        # Reclassify multiple fields
        if node.getPropertyValue("mode") == "Multiple":
            fields = [str(field) for field in node.getPropertyValue("fields") if str(field) in input_cols]
            string_conversion += "cols = " + str(fields) + "\n"
            if prev_var_name != var_name:
                string_conversion += var_name + " = " + prev_var_name + "\n"
                prev_var_name = var_name
            # Reclassify logic inserted into for loop iterating over columns
            string_conversion += '''for col_name in cols:\n\t'''
            # Reclassified field name is the variable in the loop
            old_field = "col_name"
            # Add suffix to field name if specified
            if not node.getPropertyValue("replace_field") and str(node.getPropertyValue("add_as")) == "Suffix":
                new_field = old_field + " + '" + str(node.getPropertyValue("name_extension")) + "'"
            # Add prefix to field name if specified
            elif not node.getPropertyValue("replace_field") and str(node.getPropertyValue("add_as")) == "Prefix":
                new_field =  "'" + str(node.getPropertyValue("name_extension")) + "' + " + old_field
            else:
                new_field = old_field
        # Reclassify single field
        else:
            # Defines field being reclassified, and the name of it after its reclassigied
            old_field = node.getPropertyValue("field")
            if "." in old_field:
                old_field = "`" + old_field + "`"
            old_field = "'" + old_field + "'"
            if node.getPropertyValue("replace_field"):
                new_field = old_field
            else:
                new_field = "'" + node.getPropertyValue("new_name") + "'"
        
        # Whether unspecified values should be replaced with a new default value
        if node.getPropertyValue("use_default"):
            default = cast(node.getPropertyValue("default"))
        else:
            default = "col(" + old_field + ")"
        
        text = var_name + " = " + prev_var_name + ".withColumn(" + new_field + ",\n\t"
        # Iterate over reclassified values
        for old_value in node.getKeyedPropertyKeys("reclassify"):
            new_value = node.getKeyedPropertyValue("reclassify", old_value)
            old_value, new_value = cast([old_value,new_value])
            # Each time a value is reclassified, a .when statement is inserted
            text += "when(\n\t\t(col(" + old_field + ") == " + old_value + ")," + new_value + "\n\t)."
        text += "otherwise(" + default + ")\n)"
        if node.getPropertyValue("mode") == "Multiple":
            text = text.replace("\n","\n\t")
        string_conversion += text + "\n"
    # ---- Excel Import ----
    elif node.getTypeName() == "excelimport":
        if node.getPropertyValue("data_range_mode") != "FirstNonBlank":
            raise Exception("Not implemented - (excelimport - FirstNonBlank)")
        elif node.getPropertyValue("blank_rows") != "StopReading":
            raise Exception("Not implemented - (excelimport - Blank Rows)")
        elif not node.getPropertyValue("read_field_names"):
            raise Exception("Not implemented - (excelimport - First row has column names)")
        path = node.getPropertyValue("full_filename")
        filename = re.findall("[\\/\\\\]([^\\/\\\\]*)$",path)[0]
        sheetname_str = ""
        # If a specific sheet in the excel is specified, this sheet name is passed as an argument to the read_file function
        if node.getPropertyValue("worksheet_mode") == "Name":
            sheetname_str = ", sheet_name='" + str(node.getPropertyValue("worksheet_name")) + "'"
        col_map = {}
        # Specifies intended dtypes for each field as an argument to the read_file function
        for col_name in node.getCachedFieldSet():
            old_type = str(node.getCachedFieldSet().getStorageType(str(col_name)))
            type_map = {
                "String":"string",
                "Integer":"integer",
                "Real":"float",
                "Date":"date",
                "Timestamp":"timestamp",
                "Time":"string"
            }
            if old_type not in type_map:
                print(str(node) + ": overriding to type " + old_type + " is not yet supported")
            col_map[str(col_name)] = type_map[old_type]
            col_map_str = ", dtype=" + str(col_map)
        # Uses read_file function to read excel file
        string_conversion += var_name + " = read_file(spark, parent_config_file['views_directory'] + '" + filename + "'" + sheetname_str + col_map_str + ", trim_spaces = parent_config_file['trim_inputs'])\n"
        prev_var_name = var_name
    # ---- Variable File ----
    elif node.getTypeName() == "variablefile":
        path = node.getPropertyValue("full_filename")
        filename = re.findall("[\\/\\\\]([^\\/\\\\]*)$",path)[0]
        # Adds csv delimiter as an arguement to read_file
        delimiter_str = ""
        if node.getPropertyValue("delimit_comma"):
            delimiter_str = "delim = ',', "
        elif node.getPropertyValue("delimit_other") and node.getPropertyValue("other") != "|":
            delimiter_str = "delim = '" + node.getPropertyValue("other") + "', "
        # If a special csv encoding is used, this is passed as an argument
        encoding_str = ""
        if "default_csv_encoding" in config and config["default_csv_encoding"]:
            encoding_str = "encoding = '" + config["default_csv_encoding"] + "', "
        string_conversion += var_name + " = read_file(spark, parent_config_file['views_directory'] + '" + filename + "', " + delimiter_str + encoding_str + "delete_invalid_chars=True, trim_spaces = parent_config_file['trim_inputs'])\n"
        # After reading, casts each column to its proper type as defined in the csv node
        for col_name in node.getKeyedPropertyKeys("custom_storage"):
            old_type = str(node.getKeyedPropertyValue("custom_storage",col_name))
            type_map = {
                "String":"string",
                "Integer":"integer",
                "Real":"float",
                "Date":"date",
            }
            if old_type not in type_map:
                print(str(node) + ": overriding to type " + old_type + " is not yet supported")
            string_conversion += var_name + " = " + var_name + ".withColumn('" + col_name + "', col('" + col_name + "').cast('" + type_map[old_type] + "'))\n"
        prev_var_name = var_name
    # ---- Rule Set ----
    elif node.getTypeName() == "applyrule":
        # List of each rule in ruleset, in order
        rules = [(i,node.getModel().getRoot().getChildAt(i).getChildAt(0)) for i in range(node.getModel().getRoot().getChildCount())]
        
        for i, rule in rules:
            # Convert rule to python
            cond = replace_calls(str(rule.getCondition().toExpr()),fields=input_cols)[0]
            # How rows that follow this rule should be classified
            result = cast(rule.getResult())
            # Each rule is first defined in a variable (eg. RULE4) as a condition
            string_conversion += "RULE" + str(i) + " = " + cond + "\n"

        # Creates placeholder column called 'DECISION_AND_SCORE', containing a tuple of the index of each rule (as previously defined), the rule's result, and the rule's confidence score
        string_conversion += '''
# Assigns "Approved"/"Refused", a score, and a rule number to each record based on below decision tree
%s = %s.withColumn("DECISION_AND_SCORE",
\t'''[1:]%(var_name,var_name)

        decision_to_num_rules = {}
        for i, rule in rules:
            # Rule result
            decision = cast(rule.getResult())
            # Rule confidence score
            score = round(rule.getAccuracy(),3)
            # Keeps track of how many rules have the same result
            if decision not in decision_to_num_rules:
                decision_to_num_rules[decision] = 0
            decision_to_num_rules[decision] += 1

            # Populates 'DECISION_AND_SCORE' with the relevent information for each rule
            string_conversion += "when(RULE" + str(i) + ", array(lit(" + decision + "),lit(" + str(decision_to_num_rules[decision]) + "),lit(" + str(score) +")))\n\t."

        otherwise = cast(node.getModel().getRoot().getResult())
        # New column: result field
        pred_value_field = str(node.getModel().getPredictedValueField())
        # New column: score field
        conf_field = str(node.getModel().getConfidenceField())
        # Breaks up array assigned by above decision tree into 3 columns: the predicted value field, the confidence field, and the rule number, dropping DECISION_AND_SCORE
        string_conversion += '''otherwise(array(lit(%s),lit(-1),lit(0.5)))
)

# Decision Column
%s = %s.withColumn("%s", col("DECISION_AND_SCORE")[0])
# Score Column
%s = %s.withColumn("%s", col("DECISION_AND_SCORE")[2].cast("float"))
%s = %s.drop("DECISION_AND_SCORE")
'''%(otherwise, var_name, var_name, pred_value_field, var_name, var_name, conf_field, var_name, var_name)
    # ---- Sample ----
    elif node.getTypeName() == "sample":
        if node.getPropertyValue("method") != "Simple":
            raise Exception("Not implemented - (sample - Complex)")
        if node.getPropertyValue("mode") != "Include":
            raise Exception("Not implemented - (sample - Discard sample)")
        
        # Sample: First
        if node.getPropertyValue("sample_type") == "First":
            string_conversion += var_name + " = " + prev_var_name + ".limit(" + str(node.getPropertyValue("first_n")) + ")\n"
        # Sample: 1-in-n
        elif node.getPropertyValue("sample_type") == "OneInN":
            string_conversion += '''\
%s = %s.withColumn("row_number",row_number().over(Window.orderBy(monotonically_increasing_id())))
%s = %s.filter(col("row_number")%%%s==0)
%s = %s.drop("row_number")
'''%(var_name,prev_var_name, var_name, var_name, str(node.getPropertyValue("one_in_n")), var_name, var_name) 
        # Sample: Random %
        elif node.getPropertyValue("sample_type") == "RandomPct":
            string_conversion += var_name + " = " + prev_var_name + ".sample(fraction=" + str(node.getPropertyValue("rand_pct")/100) + ")\n"
        
        # If maximum sample size specified, cuts off the sample
        if node.getPropertyValue("use_max_size") and node.getPropertyValue("sample_type") != "First":
            string_conversion += var_name + " = " + var_name + ".limit(" + str(node.getPropertyValue("maximum_size")) + ")\n"
    # ---- Statistics Import ----
    elif node.getTypeName() == "statisticsimport":
        filename = re.findall("[\\/\\\\]([^\\/\\\\]*)$",node.getPropertyValue("full_filename"))[0]
        if "." in filename:
            file_extension = re.search("\.[^\.]+$",filename).group(0)
            filename = filename[:-len(file_extension)]
        # Python cannot read .sav files. .sav files must be converted to excel beforehand
        string_conversion += var_name + " = read_file(spark, parent_config_file['views_directory'] + '" + filename + ".xlsx', trim_spaces = parent_config_file['trim_inputs'])\n"
        prev_var_name = var_name
    # ---- Transpose ----
    elif node.getTypeName() == "transpose":
        # Only Transpose method: Fields to records is implemented
        if node.getPropertyValue("transpose_method") != "vartocase":
            raise Exception("Transpose node " + node + " type not implemented yet")
        # Can only handle one index column
        if len(node.getPropertyValue("transpose_vartocase_idfields")) != 1:
            raise Exception(node + " does not have 1 id field")
        index_col = str(list(node.getPropertyValue("transpose_vartocase_idfields"))[0])
        if "." in index_col:
            index_col_quotes = "`" + index_col + "`"
        else:
            index_col_quotes = index_col
        index_col = cast(index_col)
        index_col_quotes = cast(index_col_quotes)
        # Columns to be transposed
        variables = [str(variable) for variable in node.getPropertyValue("transpose_vartocase_valfields")]
        # Block of code that transposes the data the same way SPSS does
        string_conversion += '''variables = %s
index_vals = [row[0] for row in %s.select(%s).collect()]
new_index_vals = index_vals*len(variables)
variable_vals = []
value_vals = []
for var in variables:
    variable_vals += [var]*len(index_vals)
    value_vals += [row[0] for row in %s.select('`' + var + '`').collect()]
new_vals = []
for i in range(len(new_index_vals)):
    value_val = value_vals[i]
    if value_val != None:
        value_val = str(value_val)
    new_vals.append([new_index_vals[i],variable_vals[i],value_val])
%s = spark.createDataFrame(new_vals,[%s,"variable","value"])
'''%(variables,prev_var_name,index_col_quotes,prev_var_name,var_name,index_col)
    # ---- C50 ----
    elif node.getTypeName() == "applyc50":
        # C50 nodes are bugged, and behave slightly differently then they say they will. This behaviour difference is not recorded, and is therefore impossible for python to replicate. To convert this node to python, replace with Rule Set equivalent by going to Generate > Rule Set
        print("WARNING: " + str(node) + " conversion is not supported")
        string_conversion += "'''WARNING: C5 node conversion is not supported'''\n"
        pred_value_field = str(node.getModel().getPredictedValueField())
        conf_field = str(node.getModel().getConfidenceField())
        # Creates two empty dummy fields (result and score), so that atleast SPSS' and pythons schemas remains consistent
        string_conversion += var_name + " = " + prev_var_name + ".withColumn('" + pred_value_field + "',lit(''))\n"
        string_conversion += var_name + " = " + var_name + ".withColumn('" + conf_field + "',lit(0))\n"
    # ---- Export Nodes ----
    elif node.getTypeName() in ["excelexport","outputfile","databaseexport","statisticsexport","extension_output"]:
        # Gets filename of export nodes that have a path
        if node.getTypeName() in ["excelexport","outputfile","statisticsexport"]:
            filename = re.findall("(?:^|[\\/\\\\])([^\\/\\\\]*)$",str(node.getPropertyValue("full_filename")))[0]
            filename_noext, ext = filename.split(".")
        # Gets file name of Parquet Output node
        elif node.getTypeName() == "extension_output":
            filename_noext, ext = re.findall("path\s*=\s*.*[\\\\/]([^\\\\/\n]+)\.(\w+)[^\\\\/\.]*\n",str(node.getPropertyValue("python_syntax")))[0]
        # Since python does not have access to the ODBC, Database export nodes are instead exported to an excel file of the same name
        else:
            filename_noext = str(node.getPropertyValue("table_name"))
            ext = "xlsx"
        
        # If node is in 'parquet_output_nodes' of config.json, export it as a parquet instead of an excel
        if node.getID() in config["parquet_output_nodes"]:
            ext = "parquet"
        # Exports statistics nodes as an excel instead of as a .sav
        elif node.getTypeName() == "statisticsexport":
            ext = "xlsx"
        
        # Field order matters in excel files, not in parquet. This puts the fields in the same order as they are in SPSS
        if ext != "parquet":
            # Reorder columns so they are the same order as spss
            quoted_input_cols = ["`" + input_col + "`" if "." in input_col else input_col for input_col in input_cols]
            string_conversion += var_name + " = " + prev_var_name + ".select(" + str(quoted_input_cols)[1:-1] + ")\n"
            string_conversion += "write_file(spark,%s,parent_config_file['python_output_directory'] + '%s.%s')\n"%(var_name,filename_noext,ext)
        else:
            string_conversion += "write_file(spark,%s,parent_config_file['python_output_directory'] + '%s.%s')\n"%(prev_var_name,filename_noext,ext)

        # If exported file has a tab column, will also export a distribution of that tab column
        tab_col_names = [col_name.lower() for col_name in ["TAB","TAB_TXT","BinNumber","Bin Number"]]
        active_tab_cols = [col_name for col_name in input_cols if col_name.lower() in tab_col_names]
        if active_tab_cols:
            col_name = active_tab_cols[0]
            string_conversion += "exportDistribution(%s, '%s', parent_config_file['python_output_directory'] + '%s_%s.png')\n"%(var_name,col_name,filename_noext,col_name.upper().replace(" ",""))
    # ---- Distribution ----
    elif node.getTypeName() == "distribution":
        col_name = node.getPropertyValue("x_field")
        if prev_var_name != var_name:
            string_conversion += var_name + " = " + prev_var_name + "\n"
        string_conversion += "exportDistribution(%s, '%s', parent_config_file['python_output_directory'] + '%s_Distribution_%s.png')\n"%(var_name,col_name,col_name,node.getID())
    elif node.getTypeName() == "":
        pass
    else:
        raise Exception("Not implemented (other type of node)")

    # --------Filter-----------
    # If node filters or renames columns afterwards (ie/ filter, merge, database, variablefile, excelimport, or statisticsimport nodes)
    if node.isKeyedProperty("include") or node.isKeyedProperty("new_name"):
        # Import nodes do not have a previous node, so must get input fields using the cached field set
        if node.getTypeName() not in ["database","variablefile","excelimport","statisticsimport"]:
            real_fields = set([str(col_name) for col_name in node.getInputDataModel()])
        else:
            real_fields = set([str(col_name) for col_name in node.getCachedFieldSet()])

        # Populates lists of columns being kept, and columns not being kept
        if node.isKeyedProperty("include"):
            to_drop = [str(col) for col in real_fields if node.getKeyedPropertyValue("include",col) == False]
            include = ["`" + str(col) + "`" if "." in col else str(col) for col in real_fields if node.getKeyedPropertyValue("include",col) == True]
        else:
            to_drop = []
            include = real_fields
        # Populates lists of columns being renamed, and what they are being renamed to
        if node.isKeyedProperty("new_name"):
            rename_keys = [str(col) for col in node.getKeyedPropertyKeys("new_name") if col in include]
            rename_vals = [str(node.getKeyedPropertyValue("new_name",col)).replace("\xc2","") for col in rename_keys]
        else:
            rename_keys = []
            rename_vals = []
        
        # If any columns are being dropped:
        if to_drop:
            if len(real_fields) // 2 < len(include):
                string_conversion += '''to_drop = %s\n%s = %s.drop(*to_drop)\n'''%(str(to_drop),var_name,prev_var_name)
            # If less kept columns then dropped columns, cleaner to just select the kept columns instead of dropping all of the dropped columns
            else:
                string_conversion += "%s = %s.select(%s)\n"%(var_name, prev_var_name, str(include)[1:-1])
            prev_var_name = var_name
        # If any columns are being renamed
        if rename_keys:
            string_conversion += '''%s = conversion_helper.rename_columns(%s,%s,%s)\n'''%(var_name,prev_var_name,str(rename_keys),str(rename_vals))

    # Distinct, User Input, Sort, Aggregate, Append, Reclassify, and Nominal Derive nodes all automatically break their converted code up with new line characters in a way that makes sense
    # For all other nodes, the tabify method will organize the converted code block
    if node.getTypeName() not in ["distinct", "userinput", "sort", "aggregate", "append","reclassify"] and (not node.getTypeName() == "derive" or node.getPropertyValue("result_type") != "Set"):
        string_conversion = tabify(string_conversion)
    return string_conversion


    


current_time=time.time()


# --------------------------Updating Persist List-----------------------------
stream = modeler.script.stream()
stream_name = stream.getName()

all_source_nodes = set()
found_devnode_1 = False
found_devnode_2 = False
found_devnode_3 = False
found_devnode_4 = False
found_devnode_5 = False

# Gets list of modules to import in python_files from config.json
all_sections = []
for run,sections in config["sections"]:
    all_sections+=sections
# Whether the stream is being broken up into multiple files, or if everything is going to be in app.py
seperate_file = len(all_sections) > 1 or model_name == "Test"
# Top of app.py. Downloads Parent_config_CLOUD.py from s3 
file_txt = '''# %s
import boto3, pytz, datetime
import os
'''%(stream_name)
# Imports all python files in python_files
for filename, end_node_list in all_sections:
    if seperate_file:
        file_txt += "from " + stream_folder_name + "." + filename + " import " + filename + "_stream\n"

# Import code block used at top of every python file
imports = '''import time, os, pytz, datetime, sys, logging, subprocess, re
from helpers import conversion_helper, udfs, transform
from helpers.conversion_helper import *
from helpers.udfs import *
from helpers.transform import *
import pandas as pd
from scipy.stats import norm
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, TimestampType, StringType, BooleanType, DoubleType, DateType
from pyspark import StorageLevel
from pyspark.sql.functions import (col, lit, when, sum as _sum, max as _max, min as _min, first, last, count as _count, array, array_distinct, array_join, collect_list, date_add, datediff, instr, lower, upper, row_number, last, year, datediff, length, trim, regexp_replace, regexp_extract, locate, rtrim, ltrim, size, split, substring, udf, greatest, least, monotonically_increasing_id, round as _round, concat, abs as _abs, unix_timestamp, isnull, isnan, to_date, to_timestamp, month, dayofmonth, lag, dense_rank)
'''
file_txt += imports
# Starts up pyspark, sets up logging, starts timer
file_txt += '''from Parent_config_CLOUD import parent_config as parent_config_file

spark = build_spark_session('%s_TRIAGE_MODEL_APP')
spark.sql('set spark.sql.caseSensitive=true')
spark.sparkContext.setLogLevel("ERROR")
logging.basicConfig(filename='app.log', filemode='w', format='%s', level=logging.INFO)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logging.info(f"Start: {datetime.datetime.now(pytz.timezone('Canada/Eastern'))}")
start_time_total = time.perf_counter()

'''%(model_name, "%(message)s")
# If stream is not being broken up, sets up persist counter
if not seperate_file:
    file_txt += '''persist_counter = int(1/parent_config_file["persist_percent"]) + 1 if parent_config_file["persist_percent"] > 0 else 1\n'''

# Cleaning up
file_txt = file_txt.replace("\r", "")
file_txt = file_txt.replace("\n\n\n","\n\n")
file_txt = file_txt.replace("\t","    ")

# Iterates through runs in 'sections' of config.json (the outer-most lists in the 'sections' parameter, eg. main, reports)
for run,sections in config["sections"]:
    node_to_all_name = {}
    # If there is more than one run (eg. both main and reports), inserts if statement so that the execution of each run can be controlled from the parent config file
    if len(config["sections"]) > 1:
        file_txt += 'if parent_config_file["run_%s"]:\n'%(run)
    # Iterates through each section within the current run
    for filename, old_end_node_list in sections:
        print(filename)
        current_time2=time.time()
        # Populates start_node_list from the end nodes of every other sections. This will stop the converter from converting nodes from other sections
        start_node_list = set(start_nodes.keys())
        for run2,sections2 in config["sections"]:
            for filename2, end_node_list2 in sections2:
                if filename2 != filename:
                    start_node_list = start_node_list.union(end_node_list2)

        # --------------------------Node Ordering----------------------------
        current_time3=time.time()

        stream = modeler.script.stream()
        ordered_nodes = set()
        input_nodes = []
        # If current section is being copied to a fresh stream before converting (to speed up conversion time)
        if split_streams:
            # If any node ids are specified in the current section (in config.json), finds these nodes in the stream. Note: these nodes cannot be in a supernode
            if old_end_node_list:
                leafs = []
                for node_id in old_end_node_list:
                    result = list(stream.findAll(IDFilter(node_id), False))
                    if result:
                        leafs.append(result[0])
                    else:
                        print("No match found for node_id: " + node_id)
            # Otherwise, finds every single leaf node in the stream
            else:
                leafs = [node for node in stream.findAll(LeafFilter(), False)]
            # Populates list of all of the nodes in this section, as well as the section's start nodes
            # These are all of the nodes upstream from the leafs without going into any of the other sections
            all_nodes = []
            for node in leafs:
                if node not in all_nodes:
                    all_nodes += orderBranchAll(node)
            # Insert all of the nodes into the temporary stream
            try:
                temp_stream.insert(stream, all_nodes + input_nodes, False)
            except:
                pass
            # Many of the nodes need to know their own schema (ie/ the fields coming into the node) to function correctly (eg. filter nodes, sort nodes, etc.). When a section is downstream from another section, and the downstream section is pasted into the temporary stream by itself, the information about what fields are coming from the upstream section is lost. To account for this, all of the end nodes of any upstream sections are replaced with a user input node that defines the exact same fields as each end node of the upstream sections
            # Iterates through each start node and replaces it with a user input node
            for node in input_nodes:
                # Finds start node in temporary stream
                temp_node = temp_stream.findByID(node.getID())
                # Creates user input node at same position
                user_input_node = temp_stream.createAt("userinput", node.getLabel(), node.getXPosition(), node.getYPosition())
                # Unlinks start node's predecessors
                for predecessor in temp_stream.predecessors(temp_node):
                    temp_stream.unlink(predecessor,temp_node)
                # Replaces start node with user input node
                temp_stream.replace(temp_node, user_input_node, True)
                output_field_set = node.getOutputFieldSet()
                col_names = [str(col_name) for col_name in output_field_set]
                # Defines the user input fields as the same as start node's output fields
                user_input_node.setPropertyValue("names",col_names)
                # For each user input field, defines the storage type the same as it is in the start node its replacing
                for col_name in col_names:
                    col_type = str(output_field_set.getStorageType(col_name))
                    user_input_node.setKeyedPropertyValue("custom_storage",col_name,col_type)
            stream = temp_stream

        # The order the nodes are converted
        node_order = []
        input_nodes = []
        source_nodes = []
        # This variable is used to make sure nodes aren't double counted when the stream splits
        ordered_nodes = set()
        # Refinds the leaf nodes, except this time looks inside super nodes
        if old_end_node_list:
            leafs = [list(stream.findAll(IDFilter(node_id), True))[0] for node_id in old_end_node_list]
        else:
            leafs = [node for node in stream.findAll(LeafFilter(), True)]
        end_node_list = old_end_node_list

        real_end_node_list = []
        # If any of the end nodes defined in the section are not real, changes them to their next real predecessor
        for node in leafs:
            if not isRealNode(node):
                node = getRealPredecessors(node)[0]
            real_end_node_list.append(node.getID())

        current_time3=time.time()
        # Puts all of the real nodes in the stream in order, one after the other. The python conversion will follow this order
        for node in leafs:
            if node not in ordered_nodes:
                node_to_real_successors[node] = []
                node_order += orderBranch(node)
        
        # --------------------------Node Naming-------------------------
        # This code sections assigns a python variable name to every node. The generator does not attempt to give each node a meaningful name; instead it just gives each node a unique name
        # The first node in the stream is assigned the variable df. Each subsequent node is given the same variable name until a split happens
        # When a split happens, the main branch is given the previous variable name. Each subsequent branch is given the same variable name, but with an incrementing index at the end of it. eg. Branch 1: df, Branch 2: df_1, Branch 3: df_2
        # If another split occurs after this split in Branch 2 or 3, another index is added to the variable name like so: df_1_a, df_1_b, etc. The next index after that one is roman numerals, and then it loops back to integers again
        # Additionally, the split node is given the suffix '_SPLIT'. This way when Branch 2 or 3 is run, the variable at the split node is saved and not overwritten by Branch 1
        # When a merge or append happens (ie/ multiple branches coming together), the variable name of the branch corresponding to the top-most tag in the Inputs tab of the merge node is assigned to the merge node
        # Additionally, all of the variable names of the other branches are released, so that they can be reused should a split happen later down in the stream
        # These rules stop variable names from running away (eg. df_3_b_i_1_a_iv_1) and keeps them as short as possible while still being unique
        current_time3=time.time()
        node_to_name_and_depth = {}
        node_to_name = {}
        available_name_nodes = set()
        names_in_use = set()
        unreserve_names = set()
        
        # Iterates through nodes in section
        for node in input_nodes + node_order:
            stream = node.getProcessorDiagram()
            supernode = node.getOwner()
            # List of predecessors and successors of current node
            predecessors = node_to_real_predecessors[node]
            successors = node_to_real_successors[node]

            # If no predecessors, then the node doesn't have to use the same name as one of its predecessors, and instead gets a new name
            if not predecessors:
                # Gets a new name of depth 0, starting with 'df'. eg. df, df1, and df2 are all depth 0. The name and its depth are stored
                node_to_name_and_depth[node] = (getAvailableName("df", 0),0)
                names_in_use.add(node_to_name_and_depth[node][0])
            # If the node has predecessors, then it must get its name from one of its predecessors
            else:
                # If the predecessor is a split node, and this node is not the main branch of the predecessor, then it won't get the predecessors name, and needs a new name
                if node not in node_to_name_and_depth:
                    # Predecessors name and depth
                    name, depth = node_to_name_and_depth[predecessors[0]]
                    # Generates new name based on predecessors name
                    node_to_name_and_depth[node] = (getAvailableName(name, depth + 1), depth+1)
                    # Reserves name
                    names_in_use.add(node_to_name_and_depth[node][0])
            
                # Unreserve predecessor names
                for predecessor in predecessors:
                    # Predecessor's name
                    name = node_to_name_and_depth[predecessor][0]
                    # If the current node is the last successor of this predecessor, then the predecessors name is not needed any longer by any of its successors, and it may be eligible to be released
                    if name in names_in_use and node == node_to_real_successors[predecessor][-1]:
                        # If this predecessor is the first predecessor of any of its successors, then the name is still needed by that successor. Otherwise, it can be released
                        # In other words, if the predecessors successor is a merge node who's top-most tag is not this predecessor, then it can be released
                        # Iterate through each of predecessor's successors to see if this is the case
                        unreserve_name = True
                        for successor in node_to_real_successors[predecessor]:
                            if node_to_real_predecessors[successor][0] == predecessor:
                                unreserve_name = False
                                break
                        # Unreserves predecessor's name
                        if unreserve_name:
                            names_in_use.remove(name)
                            # If predecessor was a split node, unreserves both the base name and the name with the '_SPLIT' suffix
                            if node_to_name[predecessor.getID()] in names_in_use:
                                names_in_use.remove(node_to_name[predecessor.getID()])

            # Iterates through nodes successors and assigns this nodes name to the first successor that doesn't already have a name
            i=0
            while i < len(successors) and node_to_real_predecessors[successors[i]][0] != node:
                i+=1
            if i < len(successors):
                node_to_name_and_depth[successors[i]] = node_to_name_and_depth[node]

            # Adds "_SPLIT" to node's variable name if the node has more than 1 successor
            if i < len(successors) and len(successors) + int(node in leafs) > 1:
                node_to_name[node.getID()] = getAvailableName(node_to_name_and_depth[node][0] + "_SPLIT", 0)
                names_in_use.add(node_to_name[node.getID()])
            else:
                node_to_name[node.getID()] = node_to_name_and_depth[node][0]
        # ---------------------------Code Writing----------------------------
        # Text to be added to app.py
        all_file_txt = ""
        # Starts timer for section and logs section name
        if seperate_file:
            all_file_txt += "start_time = time.perf_counter()\n"
            all_file_txt += "logging.info('" + filename + "')\n"
        # Assigns variable name to each of the section's outputs to be used in app.py. Appends section name to end of each variable to ensure uniqueness
        for node_id in real_end_node_list + end_node_list:
            node_to_all_name[node_id] = node_to_name[node_id] + "_" + filename
        # This is used when a section from a different run has an input that is the output of a section not from that run. Because the runs are run independently, this output will not exist. Instead it is assumed that the outputs from the other runs sections were cached as parquet nodes, and it will attempt to read this parquet node
        for node in input_nodes:
            if node.getID() not in node_to_all_name:
                node_to_all_name[node.getID()] = node_to_name[node.getID()] + "_INPUT"
                all_file_txt += node_to_all_name[node.getID()] + " = read_file(spark, parent_config_file[\"substream_output_directory\"] + \"" + start_nodes[node.getID()] + ".parquet\")\n"
        # Variable names of dataframes passed into the section (in section file)
        func_params = [node_to_name[input_node.getID()] for input_node in input_nodes]
        # Variable names of dataframes returned from this section (in section file)
        return_vars = [node_to_name[node_id] for node_id in real_end_node_list]
        # Variable names of dataframes returned from this section (in app.py)
        func_output = [node_to_all_name[node_id] for node_id in real_end_node_list]
        # Variable names of dataframes passed into the section (in app.py)
        func_args = [node_to_all_name[input_node.getID()] for input_node in input_nodes]
        file = ""
        # Adds top part of section file, including imports, the function definition, setting up pyspark, and setting up the persist counter
        if seperate_file:
            file += "# " + stream_name + "\n"
            file += imports
            file += "def " + filename + "_stream(" + str(["parent_config_file"] + func_params)[1:-1].replace("'","")  + "):"
            file += '''
    spark = conversion_helper.build_spark_session("%s_TRIAGE_MODEL")
    spark.sql('set spark.sql.caseSensitive=true')
    spark.sparkContext.setLogLevel("ERROR")
    if "persist_percent_sections" in parent_config_file and "%s" in parent_config_file['persist_percent_sections']:
        persist_percent = parent_config_file['persist_percent_sections']['%s']
    else:
        persist_percent = parent_config_file['persist_percent']
    persist_counter = int(1/persist_percent) + 1 if persist_percent > 0 else 1

'''%(model_name,filename,filename)
        func_body = ""
        # Iterates through each node in the node_order, converting each node to python and pasting the conversion one after the other in one big string
        for node in node_order:
            # Only convert the node if it is a real node, and not in another section
            if node.getID() not in start_node_list and isRealNode(node):
                stream = node.getProcessorDiagram()
                # Convert node to python code (if its not in the ManualCodeMap.py file)
                if node.getID() not in manual_code_map_expanded:
                    current_time3=time.time()
                    try:
                        string_conversion = convert_node(node)
                    except Exception, e:
                        raise Exception(str(node) + ": " + str(e))
                # If the node id is in the ManualCodeMap.py file, takes the manual conversion from the file instead of the automatic conversion
                else:
                    string_conversion = manual_code_map_expanded[node.getID()].strip() + "\n"
                    string_conversion = string_conversion.replace("<df>",node_to_name[node.getID()])
                # If returned code is non-empty, append to string
                if re.search("[^\t\s\n]",string_conversion):
                    string_conversion = "\n# " + str(node) + "\n" + string_conversion
                    # Add try_persist at the end to check if the node meets the conditions to be persisted
                    string_conversion += '''%s, persist_counter = try_persist(spark,%s,parent_config_file,\'\'\'%s\'\'\',persist_counter,"%s")\n'''%(node_to_name[node.getID()],node_to_name[node.getID()],str(node),filename)
                    func_body += string_conversion

        # Add return statement to section file, returning the endpoint dataframes
        if seperate_file:
            func_body += "\nreturn " + str(return_vars).replace("'","")
            func_body = func_body.replace("\n","\n\t")
        func_body = func_body.replace("\n\n\n","\n\n")
        file += func_body
        # Cleaning up
        file = file.replace("\r", "")
        file = file.replace("\n\n\n","\n\n")
        file = file.replace("\t","    ")
        # Write converted section to python file
        if seperate_file:
            f = open(PATH + model_name + "/" + stream_folder_name + "/" + filename + ".py", 'w')
            f.writelines(file)
            f.close()
        # Adds section function call to app.py
        if seperate_file:
            all_file_txt += str(func_output).replace("'","") + " = " + filename + "_stream(" + str(["parent_config_file"] + func_args)[1:-1].replace("'","")  + ")\n"
        # If the stream is not being broken up into sections, translates the app.py variables into the section variables, and then back to the app.py variables when its finished
        else:
            if func_args:
                all_file_txt += str(func_params).replace("'","") + " = " + str(func_args).replace("'","") + "\n"
            all_file_txt += file
            if func_output:
                all_file_txt += str(func_output).replace("'","") + " = " + str(return_vars).replace("'","") + "\n"
        
        # Caches each section output to parquet using write_read_file function
        for i in range(len(func_output)):
            if end_node_list[i] in export_nodes[filename]:
                all_file_txt += "%s = write_read_file(spark, %s, parent_config_file['substream_output_directory'] + '%s.parquet')\n"%(func_output[i],func_output[i],func_output[i])
            # Maps section output to variable name, in case it needs to be used as an input node in another run
            start_nodes[end_node_list[i]] = func_output[i]
        # Logs how long the section took to run
        if seperate_file:
            all_file_txt += "logging.info(f'" + filename + " Execution Time is: {round((time.perf_counter()-start_time)/60,2)} minutes')\n"
        # Clean up
        if len(config["sections"]) > 1:
            all_file_txt = "\t" + all_file_txt.replace("\n","\n\t")
        all_file_txt += "\n"
        
        file_txt += all_file_txt

        # Clears the temporary stream in preparation for the next section
        if split_streams:
            temp_stream.clear()
        
        # Keeps track of section source nodes to later be used in creating the cloud_view_generator.py file
        all_source_nodes |= set(source_nodes)
        
        # How long the section took to convert
        print filename + " Execution Time is: ", round((time.time()-current_time2)/60,2), " minutes"
# How long the whole thing took to run
file_txt += "\nlogging.info(f'Total Execution Time is: {round((time.perf_counter()-start_time_total)/60,2)} minutes')"
# Writes app.py string to app.py
f = open(PATH + model_name + "/app.py", 'w')
f.writelines(file_txt)
f.close()
# Closes temporary stream
if split_streams:
    temp_stream.close()

# How long it took for all of the code to be converted
print "Program Execution Time is: ", round((time.time()-current_time)/60,2), " minutes"