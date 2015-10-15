import json
import logging
from django.core.management.base import BaseCommand, CommandError
from optparse import make_option
from django.conf import settings
from opaque_keys import InvalidKeyError
from opaque_keys.edx.keys import CourseKey
from xmodule.modulestore import ModuleStoreEnum
from xmodule.modulestore.django import modulestore
import datetime

from django.contrib.auth.models import User
from student.models import CourseEnrollment

from py2neo import Graph, Node, Relationship, authenticate

logger = logging.getLogger('dump_to_neo4j')

def serialize_course(course):
    pass


class Command(BaseCommand):
    help = "Dump course items into a graph database"

    course_option = make_option(
        '--course',
        action='store',
        dest='course',
        default=False,
        help='--course <id> required, e.g. course-v1:org+course+run'
    )
    dump_all = make_option(
        '--all',
        action='store_true',
        dest='dump_all',
        default=False,
        help='dump all courses'
    )
    clear_all_first = make_option(
        '--clear-all-first',
        action='store_true',
        dest='clear_all_first',
        default=False,
        help='delete graph db before dumping'
    )

    option_list = BaseCommand.option_list + (course_option, dump_all, clear_all_first)

    def handle(self, *args, **options):
        graph = Graph(settings.NEO4J_URI)

        if options['verbosity']:
            # setup verbosity for third party libraries
            set_logging(options['verbosity'], ['httpstream', 'py2neo'], 4)
            # setup current module
            set_logging(options['verbosity'], ['dump_to_neo4j'])

        if options['clear_all_first']:
            logger.info("Clearing the database...")
            graph.delete_all()

        if options['dump_all']:
            courses = modulestore().get_courses()
            for course in courses:
                import_course(course, graph)
        elif options['course']:
            course_id = options['course']
            try:
                course_key = CourseKey.from_string(course_id)
            except InvalidKeyError:
                raise CommandError('invalid key "{}"'.format(course_id))
            course = modulestore().get_course(course_key)
            import_course(course, graph)

        logger.info('Done.')


def create_node(labels, fields):
    for key, value in fields.iteritems():
        if isinstance(value, dict):
            fields[key] = json.dumps(value)
        elif isinstance(value, list):
            fields[key] = unicode(value)
        elif isinstance(value, datetime.timedelta):
            fields[key] = value.seconds
    try:
        node = Node(*labels, **fields)
    except:
        import pdb; pdb.set_trace()
        raise
    return node


def create_xblock_node(block_type, fields):
    fields['xblock_type'] = block_type
    return create_node(['xblock', block_type], fields)


def import_course(course, graph):
    node_map = {}
    logger.info(u'working on course ' + unicode(course.id))
    # first pass will create graph nodes and key-node mapping,
    # which will be used for searching in the second pass
    items = modulestore().get_items(course.id)
    course_node = None
    for item in items:
        # convert all fields to a dict and filter out parent field
        fields = dict(
            (field, field_value.read_from(item))
            for (field, field_value) in item.fields.iteritems()
            if field not in ['parent', 'children']
        )
        block_type = item.scope_ids.block_type
        node = create_xblock_node(block_type, fields)
        node_map[unicode(item.location)] = node
        if block_type == 'course':
            course_node = create_node(["courseContainer"], fields)
            graph.create(course_node)
    graph.create(*node_map.values())

    # second pass
    relationships = []
    for item in items:
        if item.has_children:
            for child in item.children:
                relationship = Relationship(node_map[unicode(item.location)], 'PARENT_OF', node_map[unicode(child)])
                relationships.append(relationship)
        if course_node:
            relationship = Relationship(node_map[unicode(item.location)], 'IN', course_node)
            relationships.append(relationship)
    graph.create(*relationships)

    # third pass
    enrollments = []
    for enrollment in CourseEnrollment.objects.filter(course_id=course.id, is_active=True):
        user = enrollment.user
        mode = enrollment.mode
        user_node = Node(
            'student',
            id=user.id,
            name=user.profile.name,
            gender=user.profile.gender,
            year_of_birth=user.profile.year_of_birth,
            level_of_education=user.profile.level_of_education,
            country=unicode(user.profile.country),
            is_staff=user.is_staff,
            is_active=user.is_active,
        )
        if course_node:
            enrollments.append(
                Relationship(user_node, "ENROLLED_IN", course_node, mode=mode)
            )
    graph.create(*enrollments)


def set_logging(level, logger_names, ratio=3):
    # convert django verbosity to python log level
    logger_level = (ratio - int(level)) * 10
    for name in logger_names:
        module_logger = logging.getLogger(name)
        module_logger.setLevel(logger_level)
