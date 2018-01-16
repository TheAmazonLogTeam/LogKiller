import re
import sys
import time
from string import digits
# from strict_rfc3339 import strict_rfc3339


class Parser:

    def parse_all_logs(self, logs):

        start = time.time()

        connection_logs = []
        command_logs = []
        standard_logs_1 = []
        standard_logs_2 = []
        unknown_logs = []

        re_standard_1 = re.compile(r'(.+)\[(\d+)\]\:\s(.+)') # Date Server Protocol[PID]: Message
        re_connection = re.compile(r'(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})')  # XXX.XXX.XXX.XXX
        re_command = re.compile(r'\((\w+)\)\s(\w+)\s\((.+)\)')  # (User) Command (Arg)
        re_standard_2 = re.compile(r'(\w+)\:\s(.+)') # Date Server [PID]: Message

        logs = list(map(lambda s: s.replace("\n", "").split(" ", 2), logs))
        remove_digits = str.maketrans('', '', digits)

        for log in logs:

            log[1] = log[1].translate(remove_digits)

            try:
                re1 = re_standard_1.search(log[2])

                # Connection logs
                try:
                    re2 = re_connection.search(re1.group(3))
                    connection_logs.append([log[0], log[1], re1.group(
                        1), re1.group(2), re2.group(1), re1.group(3)])

                except:

                    # Command logs
                    try:
                        re2 = re_command.search(re1.group(3))

                        command_logs.append([log[0], log[1], re1.group(1),
                            re1.group(2), re2.group(1), re2.group(2), re2.group(3)])

                    except:
                        standard_logs_1.append(
                            [log[0], log[1], re1.group(1), re1.group(2), re1.group(3)])

            except:

                # Standard logs 2
                try:
                    re1=re_standard_2.search(log[2])
                    standard_logs_2.append(
                        [log[0], log[1], re1.group(1), re1.group(2)])

                # Unknown logs
                except:
                    unknown_logs.append(log)

        p_connection=len(connection_logs) / len(logs)
        P_standard_1=len(standard_logs_1) / len(logs)
        P_standard_2=len(standard_logs_2) / len(logs)
        p_command=len(command_logs) / len(logs)
        p_unknown=len(unknown_logs) / len(logs)

        end=time.time()

        print('\n[Sucess] Logs Parsed : Connection[%.2f%%] Command[%.2f%%] Standard1[%.2f%%] Standard2[%.2f%%] Other[%.2f%%] within %.2f s' % (
            p_connection*100, p_command*100, P_standard_1*100, P_standard_2*100, p_unknown*100, end-start))

        return connection_logs, command_logs, standard_logs_1, standard_logs_2, unknown_logs
