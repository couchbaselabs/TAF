import gzip
import json
import random
import string
import zlib

from random import choice
from string import ascii_uppercase
from string import ascii_lowercase
from string import digits
from java.lang import String
from java.nio.charset import StandardCharsets
from data import FIRST_NAMES, LAST_NAMES, DEPT, LANGUAGES
from com.couchbase.client.java.json import JsonObject
from reactor.util.function import Tuples
import copy


letters = ascii_uppercase + ascii_lowercase + digits


def doc_generator(key, start, end,
                  key_size=8, mix_key_size=False,
                  doc_size=256, doc_type="json",
                  target_vbucket=None, vbuckets=1024,
                  mutation_type="ADD", mutate=0,
                  randomize_doc_size=False, randomize_value=False,
                  randomize=False):

    # Defaults to JSON doc_type
    template_obj = JsonObject.create()
    template_obj.put("age", 5)
    template_obj.put("name", "james")
    template_obj.put("body", None)
    template_obj.put("mutated", mutate)
    template_obj.put("mutation_type", mutation_type)

#     if doc_type in ["string", "binary"]:
#         template_obj = 'age:{0}, first_name: "{1}", body: "{2}", ' \
#                    'mutated:  %s, mutation_type: "%s"' \
#                    % (mutate, mutation_type)
    if target_vbucket:
        return DocumentGeneratorForTargetVbucket(
            key, template_obj,
            start=start, end=end,
            key_size=key_size, mix_key_size=mix_key_size,
            doc_size=doc_size, doc_type=doc_type,
            target_vbucket=target_vbucket, vbuckets=vbuckets,
            randomize_doc_size=randomize_doc_size,
            randomize_value=randomize_value,
            randomize=randomize)
    return DocumentGenerator(key, template_obj,
                             start=start, end=end,
                             key_size=key_size, mix_key_size=mix_key_size,
                             doc_size=doc_size, doc_type=doc_type,
                             target_vbucket=target_vbucket, vbuckets=vbuckets,
                             randomize_doc_size=randomize_doc_size,
                             randomize_value=randomize_value,
                             randomize=randomize)


def sub_doc_generator(key, start, end, doc_size=256,
                      target_vbucket=None, vbuckets=1024, key_size=8):
    first_name = ['james', 'sharon']
    last_name = [''.rjust(doc_size - 10, 'a')]
    city = ["Chicago", "Dallas", "Seattle", "Aurora", "Columbia"]
    state = ["AL", "CA", "IN", "NV", "NY"]
    pin_code = [135, 246, 396, 837, 007]
    template = '{{ "full_name.first": "{0}", "full_name.last": "{1}", \
                   "addr.city": "{2}", "addr.state": "{3}", \
                   "addr.pincode": {4} }}'
    return SubdocDocumentGenerator(key, template,
                                   first_name, last_name,
                                   city, state, pin_code,
                                   start=start, end=end,
                                   target_vbucket=target_vbucket,
                                   vbuckets=vbuckets,
                                   key_size=key_size)


def sub_doc_generator_for_edit(key, start, end, template_index=0,
                               target_vbucket=None, vbuckets=1024,
                               key_size=8):
    template = list()
    template.append('{{ "full_name.last": "LastNameUpdate", \
                        "addr.city": "CityUpdate", \
                        "addr.pincode": "TypeChange", \
                        "todo.morning": [\"get\", \"up\"] }}')
    template.append('{{ "full_name.first": "FirstNameUpdate", \
                        "addr.state": "NewState", \
                        "todo.night": [1, \"nothing\", 3] }}')
    template.append('{{ "full_name.first": "", \
                        "addr": "", \
                        "full_name.last": "" }}')
    return SubdocDocumentGenerator(key, template[template_index],
                                   start=start, end=end,
                                   target_vbucket=target_vbucket,
                                   vbuckets=vbuckets,
                                   key_size=key_size)


class KVGenerator(object):
    def __init__(self, name):
        self.name = name
        self.itr = 0
        self.start = 0
        self.end = 0
        self.random = random.Random()
        self.randomize_doc_size = False
        self.randomize_value = False
        self.randomize = False
        self.mix_key_size = False
        self.doc_type = "json"
        self.key_size = 8
        self.doc_size = 256
        self.body = [''.rjust(self.doc_size - 10, 'a')][0]

    def has_next(self):
        return self.itr < self.end

    def next_key(self, doc_index):
        return "%s-%s" % (self.name,
                          str(abs(doc_index)).zfill(self.key_size
                                                    - len(self.name)
                                                    - 1))

    def next(self):
        raise NotImplementedError

    def reset(self):
        self.itr = self.start

    def __iter__(self):
        return self

    def __len__(self):
        return self.end - self.start


class DocumentGenerator(KVGenerator):
    """ An idempotent document generator."""
    def __init__(self, name, template, *args, **kwargs):
        """Initializes the document generator

        Example:
        Creates 10 documents, but only iterates through the first 5.

        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen = DocumentGenerator('test_docs', template, age, first,
                                start=0, end=5)

        Args:
            name: The key name prefix
            template: A formated string that can be used to generate documents
            *args: Each arg is list for the corresponding param in the template
                   In the above example age[2] appears in the 3rd document
            *kwargs: Special constrains for the document generator,
                     currently start and end are supported
        """
        self.args = args
        self.kwargs = kwargs
        self.template = template
        KVGenerator.__init__(self, name)

        if 'start' in kwargs:
            self.start = kwargs['start']
            self.itr = kwargs['start']

        if 'end' in kwargs:
            self.end = kwargs['end']

        if 'doc_type' in kwargs:
            self.doc_type = kwargs['doc_type']

        if 'key_size' in kwargs:
            self.key_size = kwargs['key_size']

        if 'doc_size' in kwargs:
            self.doc_size = kwargs['doc_size']
            self.body = [''.rjust(self.doc_size - 10, 'a')][0]

        if 'randomize_doc_size' in kwargs:
            self.randomize_doc_size = kwargs['randomize_doc_size']

        if 'randomize_value' in kwargs:
            self.randomize_value = kwargs['randomize_value']
            random.seed(name)
            self.random_string = [''.join(random.choice(letters)
                                          for _ in range(4*1024))][0]
            self.len_random_string = len(self.random_string)

        if 'randomize' in self.kwargs:
            self.randomize = self.kwargs["randomize"]

        if 'mix_key_size' in kwargs:
            self.mix_key_size = kwargs['mix_key_size']

    """Creates the next generated document and increments the iterator.

    Returns:
        The document generated"""

    def next(self):
        if self.itr >= self.end:
            raise StopIteration
        template = self.template
#         template = copy.deepcopy(self.template)
        seed_hash = self.name + '-' + str(abs(self.itr))
        self.random.seed(seed_hash)
        if self.randomize:
            for k in template.getNames():
                if k in self.kwargs:
                    template.put(k, self.random.choice(self.kwargs[k]))

        doc_size = self.doc_size
        if self.randomize_doc_size:
            doc_size = self.random.randint(0, self.doc_size)
            self.body = [''.rjust(doc_size - 10, 'a')][0]

        if doc_size and self.randomize_value:
            _slice = int(self.random.random()*self.len_random_string)
            self.body = (self.random_string *
                         (doc_size/self.len_random_string+2)
                         )[_slice:doc_size + _slice]

        template.put("body", self.body)

        if self.doc_type.find("binary") != -1:
            template = String(template).getBytes(StandardCharsets.UTF_8)

        if self.name == "random_keys":
            """ This will generate a random ascii key with 12 characters """
            doc_key = ''.join(self.random.choice(letters)
                              for _ in range(self.key_size))
        elif self.mix_key_size:
            doc_key = ''.join(self.random.choice(letters)
                              for _ in range(self.random.randint(self.key_size,
                                                                 250)))
        else:
            doc_key = self.next_key(self.itr)
        self.itr += 1
        return doc_key, template


class SubdocDocumentGenerator(KVGenerator):
    """ An idempotent document generator."""

    def __init__(self, name, template, *args, **kwargs):
        """Initializes the Sub document generator

        Example:
        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first.name": "{1}" }}'
        gen = SubdocDocumentGenerator('test_docs', template, age, first,
                                start=0, end=5)

        Args:
            name: The key name prefix
            template: A formated string that can be used to generate documents
            *args: A list for each argument in the template
            *kwargs: Special constrains for the document generator
        """
        self.args = args
        self.template = template
        self.doc_type = "json"
        self.doc_keys = dict()
        self.doc_keys_len = 0
        self.key_counter = 0
        self.key_size = 0
        self.target_vbucket = None
        self.vbuckets = None

        KVGenerator.__init__(self, name)

        if 'start' in kwargs:
            self.start = kwargs['start']
            self.itr = kwargs['start']

        if 'end' in kwargs:
            self.end = kwargs['end']

        if 'doc_type' in kwargs:
            self.doc_type = kwargs['doc_type']

        if 'target_vbucket' in kwargs:
            self.target_vbucket = kwargs['target_vbucket']

        if 'vbuckets' in kwargs:
            self.vbuckets = kwargs['vbuckets']

        if 'key_size' in kwargs:
            self.key_size = kwargs['key_size']

        if self.target_vbucket:
            self.key_counter = self.start
            self.create_key_for_vbucket()

    def create_key_for_vbucket(self):
        while self.doc_keys_len < self.end:
            doc_key = self.next_key(self.key_counter)
            tem_vb = (((zlib.crc32(doc_key)) >> 16) & 0x7fff) & \
                     (self.vbuckets-1)
            if tem_vb in self.target_vbucket:
                self.doc_keys.update({self.start+self.doc_keys_len: doc_key})
                self.doc_keys_len += 1
            self.key_counter += 1
        self.end = self.start + self.doc_keys_len

    """Creates the next generated document and increments the iterator.
    Returns:
        The document generated"""
    def next(self):
        if self.itr >= self.end:
            raise StopIteration

        doc_args = list()
        rand_hash = self.name + '-' + str(self.itr)
        self.random.seed(rand_hash)
        for arg in self.args:
            value = self.random.choice(arg)
            doc_args.append(value)
        doc = self.template.format(*doc_args).replace('\'', '"') \
            .replace('True', 'true') \
            .replace('False', 'false') \
            .replace('\\', '\\\\')
        json_val = json.loads(doc)
        return_val = []
        for path, value in json_val.items():
            return_val.append((path, value))

        if self.target_vbucket is not None:
            doc_key = self.doc_keys[self.itr]
        elif self.name == "random_keys":
            """ This will generate a random ascii key with 12 characters """
            seed_hash = self.name + '-' + str(self.itr)
            self.random.seed(seed_hash)
            doc_key = ''.join(self.random.choice(
                              ascii_uppercase + ascii_lowercase + digits)
                              for _ in range(12))
        else:
            doc_key = self.next_key(self.itr)

        self.itr += 1
        return doc_key, return_val


class DocumentGeneratorForTargetVbucket(KVGenerator):
    """ An idempotent document generator."""
    def __init__(self, name, template, *args, **kwargs):
        """Initializes the document generator

        Example:
        Creates 10 documents, but only iterates through the first 5.

        age = range(5)
        first = ['james', 'sharon']
        template = '{{ "age": {0}, "first_name": "{1}" }}'
        gen = DocumentGenerator('test_docs', template, age, first,
                                start=0, end=5)

        Args:
            name: The key name prefix
            template: A formated string that can be used to generate documents
            *args: Each arg is list for the corresponding param in the template
                   In the above example age[2] appears in the 3rd document
            *kwargs: Special constrains for the document generator,
                     currently start and end are supported
        """
        self.args = args
        self.kwargs = kwargs
        self.template = template
        self.doc_type = "json"
        self.doc_keys = dict()
        self.doc_keys_len = 0
        self.key_size = 0
        self.key_counter = 1

        KVGenerator.__init__(self, name)

        if 'start' in kwargs:
            self.start = kwargs['start']
            self.itr = kwargs['start']

        if 'end' in kwargs:
            self.end = kwargs['end']

        if 'doc_type' in kwargs:
            self.doc_type = kwargs['doc_type']

        if 'vbuckets' in kwargs:
            self.vbuckets = kwargs['vbuckets']

        if 'target_vbucket' in kwargs:
            self.target_vbucket = kwargs['target_vbucket']

        if 'key_size' in kwargs:
            self.key_size = kwargs['key_size']

        if 'doc_size' in kwargs:
            self.doc_size = kwargs['doc_size']

        if 'randomize_doc_size' in kwargs:
            self.randomize_doc_size = kwargs['randomize_doc_size']

        if 'randomize_value' in kwargs:
            self.randomize_value = kwargs['randomize_value']
            random.seed(name)
            self.random_string = [''.join(random.choice(letters)
                                          for _ in range(4*1024))][0]
            self.len_random_string = len(self.random_string)

        if 'randomize' in self.kwargs:
            self.randomize = self.kwargs["randomize"]

        if 'mix_key_size' in kwargs:
            self.mix_key_size = kwargs['mix_key_size']

        self.key_counter = self.start
        self.create_key_for_vbucket()

    def create_key_for_vbucket(self):
        while self.doc_keys_len < self.end:
            doc_key = self.next_key(self.key_counter)
            tem_vb = (((zlib.crc32(doc_key)) >> 16) & 0x7fff) & \
                (self.vbuckets-1)
            if tem_vb in self.target_vbucket:
                self.doc_keys.update({self.start+self.doc_keys_len: doc_key})
                self.doc_keys_len += 1
            self.key_counter += 1
        self.end = self.start + self.doc_keys_len

    """
    Creates the next generated document and increments the iterator.
    Returns:
       The document generated
    """
    def next(self):
        if self.itr > self.end:
            raise StopIteration

        rand_hash = self.name + '-' + str(self.itr)
        self.random.seed(rand_hash)
        if self.randomize:
            for k in self.template.getNames():
                if k in self.kwargs:
                    self.template.put(k, self.random.choice(self.kwargs[k]))

        if self.randomize_doc_size:
            doc_size = self.random.randint(0, self.doc_size)
            self.body = [''.rjust(doc_size - 10, 'a')][0]

        if self.doc_size and self.randomize_value:
            _slice = int(self.random.random()*self.len_random_string)
            self.body = (self.random_string *
                         (self.doc_size/self.len_random_string+2)
                         )[_slice:self.doc_size + _slice]
        doc_key = self.doc_keys[self.itr]
        self.itr += 1
        return doc_key, self.template


class BlobGenerator(KVGenerator):
    def __init__(self, name, seed, value_size, start=0, end=10000):
        KVGenerator.__init__(self, name, start, end)
        self.seed = seed
        self.value_size = value_size
        self.itr = self.start
        self.doc_type = "string"

    def next(self):
        if self.itr >= self.end:
            raise StopIteration

        if self.name == "random_keys":
            key = ''.join(choice(ascii_uppercase+ascii_lowercase+digits)
                          for _ in range(12))
        else:
            key = self.name + str(self.itr)
        if self.value_size == 1:
            value = random.choice(string.letters)
        else:
            value = self.seed + str(self.itr)
            extra = self.value_size - len(value)
            if extra > 0:
                value += 'a' * extra
        self.itr += 1
        return key, value


class BatchedDocumentGenerator(object):

    def __init__(self, document_generator, batch_size_int=100):
        self._doc_gen = document_generator
        self._batch_size = batch_size_int
        self._doc_gen.random = random.Random()
        self.doc_type = document_generator.doc_type

        if self._batch_size <= 0:
            raise ValueError("Invalid Batch size {0}".format(self._batch_size))

    def has_next(self):
        return self._doc_gen.has_next()

    def next_batch(self):
        self.count = 0
        key_val = []
        while self.count < self._batch_size and self.has_next():
            key, val = self._doc_gen.next()
            key_val.append(Tuples.of(key, val))
            self.count += 1
        return key_val


class JSONNonDocGenerator(KVGenerator):
    """
    Values can be arrays, integers, strings
    """
    def __init__(self, name, values, start=0, end=10000):
        KVGenerator.__init__(self, name, start, end)
        self.values = values
        self.itr = self.start

    def next(self):
        if self.itr >= self.end:
            raise StopIteration

        key = self.name + str(self.itr)
        index = self.itr
        while index > len(self.values):
            index = index - len(self.values)
        value = json.dumps(self.values[index-1])
        self.itr += 1
        return key, value


class Base64Generator(KVGenerator):
    def __init__(self, name, values, start=0, end=10000):
        KVGenerator.__init__(self, name, start, end)
        self.values = values
        self.itr = self.start

    def next(self):
        if self.itr >= self.end:
            raise StopIteration

        key = self.name + str(self.itr)
        index = self.itr
        while index > len(self.values):
            index = index - len(self.values)
        value = self.values[index-1]
        self.itr += 1
        return key, value


class JsonDocGenerator(KVGenerator):

    def __init__(self, name, op_type="create", encoding="utf-8",
                 *args, **kwargs):
        """Initializes the JSON document generator
        gen =  JsonDocGenerator(prefix, encoding="utf-8",start=0,end=num_items)

        Args:
            prefix: prefix for key
            encoding: utf-8/ascii/utf-16 encoding of JSON doc
            *args: A list for each argument in the template
            *kwargs: Special constrains for the document generator

        Sample doc:
                    {
                      "salary": 75891.68,
                      "name": "Safiya Morgan",
                      "dept": "Support",
                      "is_manager": true,
                      "mutated": 0,
                      "join_date": "1984-05-22 07:28:00",
        optional-->   "manages": {
                        "team_size": 6,
                        "reports": [
                          "Basha Taylor",
                          "Antonia Cox",
                          "Winta Campbell",
                          "Lilith Scott",
                          "Beryl Miller",
                          "Ambika Reed"
                        ]
                      },
                      "languages_known": [
                        "English",
                        "Spanish",
                        "German"
                      ],
                      "emp_id": 10000001,
                      "email": "safiya_1@mcdiabetes.com"
                    }
        """
        self.args = args
        self.name = name
        self.gen_docs = {}
        self.encoding = encoding
        self.doc_type = "json"

        size = 0
        if not len(self.args) == 0:
            size = 1
            for arg in self.args:
                size *= len(arg)

        KVGenerator.__init__(self, name, 0, size)
        random.seed(0)

        if 'start' in kwargs:
            self.start = int(kwargs['start'])
            self.itr = int(kwargs['start'])
        if 'end' in kwargs:
            self.end = int(kwargs['end'])

        if op_type == "create":
            for count in xrange(self.start+1, self.end+1, 1):
                emp_name = self.generate_name()
                doc_dict = {
                            'emp_id': str(10000000+int(count)),
                            'name': emp_name,
                            'dept': self.generate_dept(),
                            'email': "%s@mcdiabetes.com" %
                                     (emp_name.split(' ')[0].lower()),
                            'salary': self.generate_salary(),
                            'join_date': self.generate_join_date(),
                            'languages_known': self.generate_lang_known(),
                            'is_manager': bool(random.getrandbits(1)),
                            'mutated': 0,
                            'type': 'emp'
                           }
                if doc_dict["is_manager"]:
                    doc_dict['manages'] = {'team_size': random.randint(5, 10)}
                    doc_dict['manages']['reports'] = []
                    for _ in xrange(0, doc_dict['manages']['team_size']):
                        doc_dict['manages']['reports'].append(self.generate_name())
                self.gen_docs[count-1] = doc_dict
        elif op_type == "delete":
            # for deletes, just keep/return empty docs with just type field
            for count in xrange(self.start, self.end):
                self.gen_docs[count] = {'type': 'emp'}

    def update(self, fields_to_update=None):
        """
            Updates the fields_to_update in the document.
            @param fields_to_update is usually a list of fields one wants to
                   regenerate in a doc during update. If this is 'None', by
                   default for this dataset, 'salary' field is regenerated.
        """
        random.seed(1)
        for count in xrange(self.start, self.end):
            doc_dict = self.gen_docs[count]
            if fields_to_update is None:
                doc_dict['salary'] = self.generate_salary()
            else:
                if 'salary' in fields_to_update:
                    doc_dict['salary'] = self.generate_salary()
                if 'dept' in fields_to_update:
                    doc_dict['dept'] = self.generate_dept()
                if 'is_manager' in fields_to_update:
                    doc_dict['is_manager'] = bool(random.getrandbits(1))
                    if doc_dict["is_manager"]:
                        doc_dict['manages'] = {'team_size': random.randint(5, 10)}
                        doc_dict['manages']['reports'] = []
                        for _ in xrange(0, doc_dict['manages']['team_size']):
                            doc_dict['manages']['reports'].append(self.generate_name())
                if 'languages_known' in fields_to_update:
                    doc_dict['languages_known'] = self.generate_lang_known()
                if 'email' in fields_to_update:
                    doc_dict['email'] = "%s_%s@mcdiabetes.com" % \
                                        (doc_dict['name'].split(' ')[0].lower(),
                                         str(random.randint(0, 99)))
                if 'manages.team_size' in fields_to_update or\
                        'manages.reports' in fields_to_update:
                    doc_dict['manages'] = {}
                    doc_dict['manages']['team_size'] = random.randint(5, 10)
                    doc_dict['manages']['reports'] = []
                    for _ in xrange(0, doc_dict['manages']['team_size']):
                        doc_dict['manages']['reports'].append(self.generate_name())
            self.gen_docs[count] = doc_dict

    def next(self):
        if self.itr >= self.end:
            raise StopIteration
        doc = self.gen_docs[self.itr]
        self.itr += 1
        return self.name+str(10000000+self.itr), \
            json.dumps(doc).encode(self.encoding, "ignore")

    def generate_join_date(self):
        import datetime
        year = random.randint(1950, 2016)
        month = random.randint(1, 12)
        day = random.randint(1, 28)
        hour = random.randint(0, 23)
        minutes = random.randint(0, 59)
        return datetime.datetime(year, month, day, hour, minutes).isoformat()

    def generate_dept(self):
        return DEPT[random.randint(0, len(DEPT)-1)]

    def generate_salary(self):
        return round(random.random()*100000 + 50000, 2)

    def generate_name(self):
        return "%s %s" % (FIRST_NAMES[random.randint(1, len(FIRST_NAMES)-1)],
                          LAST_NAMES[random.randint(1, len(LAST_NAMES)-1)])

    def generate_lang_known(self):
        count = 0
        lang = []
        while count < 3:
            lang.append(LANGUAGES[random.randint(0, len(LANGUAGES)-1)])
            count += 1
        return lang


class WikiJSONGenerator(KVGenerator):

    def __init__(self, name, lang='EN', encoding="utf-8", op_type="create",
                 *args, **kwargs):

        """Wikipedia JSON document generator

        gen = WikiJSONGenerator(prefix, lang="DE","encoding="utf-8",
                                start=0,end=1000)
        Args:
            prefix: prefix for key
            encoding: utf-8/ascii/utf-16 encoding of JSON doc
            *args: A list for each argument in the template
            *kwargs: Special constrains for the document generator

        ** For EN, generates 20000 unique docs, and then duplicates docs **
        ** For ES, DE and FR, generates 5000 unique docs and then duplicates **


        Sample EN doc:

        {
           "revision": {
              "comment": "robot Modifying: [[bar:Apr\u00fc]]",
              "timestamp": "2010-05-13T20:42:11Z",
              "text": {
                 "@xml:space": "preserve",
                 "#text": "'''April''' is the fourth month of the year with 30
                 days. The name April comes from that Latin word ''aperire''
                 which means \"to open\". This probably refers to growing plants
                 in spring. April begins on the same day of week as ''[[July]]''
                 in all years and also ''[[January]]'' in leap years.\n\nApril's
                flower is the Sweet Pea and ...<long text>
                },
              "contributor": {
                 "username": "Xqbot",
                 "id": "40158"
              },
              "id": "2196110",
              "minor": null
           },
           "id": "1",
           "title": "April"
           "mutated": 0,
        }


        """

        self.args = args
        self.name = name
        self.gen_docs = {}
        self.encoding = encoding
        self.lang = lang

        size = 0
        if not len(self.args) == 0:
            size = 1
            for arg in self.args:
                size *= len(arg)

        KVGenerator.__init__(self, name, 0, size)
        random.seed(0)

        if 'start' in kwargs:
            self.start = int(kwargs['start'])
            self.itr = int(kwargs['start'])
        if 'end' in kwargs:
            self.end = int(kwargs['end'])

        if op_type == "create":
            self.read_from_wiki_dump()
        elif op_type == "delete":
            # for deletes, just keep/return empty docs with just type field
            for count in xrange(self.start, self.end):
                self.gen_docs[count] = {'type': 'wiki'}

    def read_from_wiki_dump(self):
        count = 0
        done = False
        while not done:
            try:
                with gzip.open("lib/couchbase_helper/wiki/{0}wiki.txt.gz"
                               .format(self.lang.lower()), "r") as f:
                    for doc in f:
                        self.gen_docs[count] = doc
                        if count >= self.end:
                            f.close()
                            done = True
                            break
                        count += 1
                    f.close()
            except IOError:
                lang = self.lang.lower()
                wiki = eval("{0}WIKI".format(self.lang))
                print ("Unable to find file lib/couchbase_helper/wiki/"
                       "{0}wiki.txt.gz. Downloading from {1}...".
                       format(lang, wiki))
                import urllib
                urllib.URLopener().retrieve(
                    wiki,
                    "lib/couchbase_helper/wiki/{0}wiki.txt.gz".format(lang))
                print "Download complete!"

    def next(self):
        if self.itr >= self.end:
            raise StopIteration
        doc = {}
        try:
            doc = eval(self.gen_docs[self.itr])
        except TypeError:
            # happens with 'delete' gen
            pass
        doc['mutated'] = 0
        doc['type'] = 'wiki'
        self.itr += 1
        return self.name+str(10000000+self.itr), \
            json.dumps(doc, indent=3).encode(self.encoding, "ignore")
