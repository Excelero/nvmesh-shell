#!/usr/bin/env python
# coding=utf-8
#
# Copyright (c) 2018 Excelero, Inc. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
#
# Author:        Andreas Krause
# Maintainer:    Andreas Krause
# Email:         andreas@excelero.com


import logging
from cmd2 import Cmd, with_argparser, with_category
import argparse
import json
import gnureadline as readline
import atexit
import sys
import os
import getpass
import paramiko
import base64
from humanfriendly.tables import format_smart_table
import humanfriendly
import time
import urllib3
from multiprocessing import Pool
import dateutil.parser
import re
import requests

__version__ = '52'

RAID_LEVELS = {
    'lvm': 'LVM/JBOD',
    '0': 'Striped RAID-0',
    '1': 'Mirrored RAID-1',
    '10': 'Striped & Mirrored RAID-10',
    'ec': 'Erasure Coding',
    'con': 'Concatenated'
}

NVME_VENDORS = {
    '0x1344': 'Micron',
    '0x15b7': 'SanDisk',
    '0x1179': 'Toshiba',
    '0x144d': 'Samsung',
    '0x1bb1': 'Seagate'
}

PROTECTION_LEVELS = {
    2: 'Full Separation',
    1: 'Minimal Separation',
    0: 'Ignore Separation'
}

FORMAT_TYPES = {
    'ec': 'format_ec',
    'legacy': 'format_raid'
}

WARNINGS = {
    'delete_volume': 'This operation will DESTROY ALL DATA on the volume selected and is IRREVERSIBLE.\nDo you want to continue? [Yes|No]: ',
    'format_drive': 'This operation will DESTROY ALL DATA on the drives and is IRREVERSIBLE.\nDo you want to continue? [Yes|No]: ',
    'force_detach_volume': 'This operation will immediately HALT ALL I/O and will impact any running applications expecting it to be available. It is recommended that all applications and/or file systems using this volume be stopped/un-mounted prior to issuing the command.\nDo you want to continue? [Yes|No]: ',
    'stop_nvmesh_client': 'This operation will HALT ALL I/O TO ALL VOLUMES in use by THE SELECTED CLIENT. It is recommended that all applications and/or file systems supported by NVMesh volumes on the clients be stopped/un-mounted prior to issuing the command.\nDo you want to continue? [Yes|No]: ',
    'stop_nvmesh_target': 'This operation will make any UNPROTECTED VOLUMES supported by drives in the selected targets IMMEDIATELY UNAVAILABLE. Any PROTECTED VOLUMES will become IMMEDIATELY DEGRADED until services are restarted or volumes are rebuilt to alternate drives in another target.\nDo you want to continue? [Yes|No]: ',
    'stop_nvmesh_manager': 'This operation will halt the running instance of NVMesh Management on the selected servers. If Management is deployed as a stand-alone instance, or this is the last running HA instance, further changes to NVMesh cluster volumes, clients, and targets will be unavailable until Management is restarted on at least one node.\nDo you want to continue? [Yes|No]: ',
    'stop_cluster': 'ALL NVMesh resources will become UNAVAILABLE and ALL IO will stop. It is recommended that all applications and/or file systems using any resource out of this cluster be stopped/un-mounted prior to issuing the command.\nDo you want to continue? [Yes|No]: ',
    'evict_drive': 'This operation will make any UNPROTECTED VOLUMES supported by this drive IMMEDIATELY UNAVAILABLE. Any PROTECTED VOLUMES will become IMMEDIATELY DEGRADED.\nDo you want to continue? [Yes|No]: '
}

__license__ = r"""GNU GENERAL PUBLIC LICENSE
Version 3, 29 June 2007
Copyright (C) 2007 Free Software Foundation, Inc. <http://fsf.org/>
Everyone is permitted to copy and distribute verbatim copies of this license document, but changing it is not allowed.
Preamble:
The GNU General Public License is a free, copyleft license for software and other kinds of works.
The licenses for most software and other practical works are designed to take away your freedom to share and change the works. By contrast, the GNU General Public License is intended to guarantee your freedom to share and change all versions of a program--to make sure it remains free software for all its users. We, the Free Software Foundation, use the GNU General Public License for most of our software; it applies also to any other work released this way by its authors. You can apply it to your programs, too.
When we speak of free software, we are referring to freedom, not price. Our General Public Licenses are designed to make sure that you have the freedom to distribute copies of free software (and charge for them if you wish), that you receive source code or can get it if you want it, that you can change the software or use pieces of it in new free programs, and that you know you can do these things.
To protect your rights, we need to prevent others from denying you these rights or asking you to surrender the rights. Therefore, you have certain responsibilities if you distribute copies of the software, or if you modify it: responsibilities to respect the freedom of others.
For example, if you distribute copies of such a program, whether gratis or for a fee, you must pass on to the recipients the same freedoms that you received. You must make sure that they, too, receive or can get the source code. And you must show them these terms so they know their rights.
Developers that use the GNU GPL protect your rights with two steps: (1) assert copyright on the software, and (2) offer you this License giving you legal permission to copy, distribute and/or modify it.
For the developers' and authors' protection, the GPL clearly explains that there is no warranty for this free software. For both users' and authors' sake, the GPL requires that modified versions be marked as changed, so that their problems will not be attributed erroneously to authors of previous versions.
Some devices are designed to deny users access to install or run modified versions of the software inside them, although the manufacturer can do so. This is fundamentally incompatible with the aim of protecting users' freedom to change the software. The systematic pattern of such abuse occurs in the area of products for individuals to use, which is precisely where it is most unacceptable. Therefore, we have designed this version of the GPL to prohibit the practice for those products. If such problems arise substantially in other domains, we stand ready to extend this provision to those domains in future versions of the GPL, as needed to protect the freedom of users.
Finally, every program is threatened constantly by software patents. States should not allow patents to restrict development and use of software on general-purpose computers, but in those that do, we wish to avoid the special danger that patents applied to a free program could make it effectively proprietary. To prevent this, the GPL assures that patents cannot be used to render the program non-free.
The precise terms and conditions for copying, distribution and modification follow.

TERMS AND CONDITIONS
0. Definitions.
“This License” refers to version 3 of the GNU General Public License.
“Copyright” also means copyright-like laws that apply to other kinds of works, such as semiconductor masks.
“The Program” refers to any copyrightable work licensed under this License. Each licensee is addressed as “you”. “Licensees” and “recipients” may be individuals or organizations.
To “modify” a work means to copy from or adapt all or part of the work in a fashion requiring copyright permission, other than the making of an exact copy. The resulting work is called a “modified version” of the earlier work or a work “based on” the earlier work.
A “covered work” means either the unmodified Program or a work based on the Program.
To “propagate” a work means to do anything with it that, without permission, would make you directly or secondarily liable for infringement under applicable copyright law, except executing it on a computer or modifying a private copy. Propagation includes copying, distribution (with or without modification), making available to the public, and in some countries other activities as well.
To “convey” a work means any kind of propagation that enables other parties to make or receive copies. Mere interaction with a user through a computer network, with no transfer of a copy, is not conveying.
An interactive user interface displays “Appropriate Legal Notices” to the extent that it includes a convenient and prominently visible feature that (1) displays an appropriate copyright notice, and (2) tells the user that there is no warranty for the work (except to the extent that warranties are provided), that licensees may convey the work under this License, and how to view a copy of this License. If the interface presents a list of user commands or options, such as a menu, a prominent item in the list meets this criterion.

1. Source Code.
The “source code” for a work means the preferred form of the work for making modifications to it. “Object code” means any non-source form of a work.
A “Standard Interface” means an interface that either is an official standard defined by a recognized standards body, or, in the case of interfaces specified for a particular programming language, one that is widely used among developers working in that language.
The “System Libraries” of an executable work include anything, other than the work as a whole, that (a) is included in the normal form of packaging a Major Component, but which is not part of that Major Component, and (b) serves only to enable use of the work with that Major Component, or to implement a Standard Interface for which an implementation is available to the public in source code form. A “Major Component”, in this context, means a major essential component (kernel, window system, and so on) of the specific operating system (if any) on which the executable work runs, or a compiler used to produce the work, or an object code interpreter used to run it.
The “Corresponding Source” for a work in object code form means all the source code needed to generate, install, and (for an executable work) run the object code and to modify the work, including scripts to control those activities. However, it does not include the work's System Libraries, or general-purpose tools or generally available free programs which are used unmodified in performing those activities but which are not part of the work. For example, Corresponding Source includes interface definition files associated with source files for the work, and the source code for shared libraries and dynamically linked subprograms that the work is specifically designed to require, such as by intimate data communication or control flow between those subprograms and other parts of the work.
The Corresponding Source need not include anything that users can regenerate automatically from other parts of the Corresponding Source.
The Corresponding Source for a work in source code form is that same work.

2. Basic Permissions.
All rights granted under this License are granted for the term of copyright on the Program, and are irrevocable provided the stated conditions are met. This License explicitly affirms your unlimited permission to run the unmodified Program. The output from running a covered work is covered by this License only if the output, given its content, constitutes a covered work. This License acknowledges your rights of fair use or other equivalent, as provided by copyright law.
You may make, run and propagate covered works that you do not convey, without conditions so long as your license otherwise remains in force. You may convey covered works to others for the sole purpose of having them make modifications exclusively for you, or provide you with facilities for running those works, provided that you comply with the terms of this License in conveying all material for which you do not control copyright. Those thus making or running the covered works for you must do so exclusively on your behalf, under your direction and control, on terms that prohibit them from making any copies of your copyrighted material outside their relationship with you.
Conveying under any other circumstances is permitted solely under the conditions stated below. Sublicensing is not allowed; section 10 makes it unnecessary.

3. Protecting Users' Legal Rights From Anti-Circumvention Law.
No covered work shall be deemed part of an effective technological measure under any applicable law fulfilling obligations under article 11 of the WIPO copyright treaty adopted on 20 December 1996, or similar laws prohibiting or restricting circumvention of such measures.
When you convey a covered work, you waive any legal power to forbid circumvention of technological measures to the extent such circumvention is effected by exercising rights under this License with respect to the covered work, and you disclaim any intention to limit operation or modification of the work as a means of enforcing, against the work's users, your or third parties' legal rights to forbid circumvention of technological measures.

4. Conveying Verbatim Copies.
You may convey verbatim copies of the Program's source code as you receive it, in any medium, provided that you conspicuously and appropriately publish on each copy an appropriate copyright notice; keep intact all notices stating that this License and any non-permissive terms added in accord with section 7 apply to the code; keep intact all notices of the absence of any warranty; and give all recipients a copy of this License along with the Program.
You may charge any price or no price for each copy that you convey, and you may offer support or warranty protection for a fee.

5. Conveying Modified Source Versions.
You may convey a work based on the Program, or the modifications to produce it from the Program, in the form of source code under the terms of section 4, provided that you also meet all of these conditions:
a) The work must carry prominent notices stating that you modified it, and giving a relevant date.
b) The work must carry prominent notices stating that it is released under this License and any conditions added under section 7. This requirement modifies the requirement in section 4 to “keep intact all notices”.
c) You must license the entire work, as a whole, under this License to anyone who comes into possession of a copy. This License will therefore apply, along with any applicable section 7 additional terms, to the whole of the work, and all its parts, regardless of how they are packaged. This License gives no permission to license the work in any other way, but it does not invalidate such permission if you have separately received it.
d) If the work has interactive user interfaces, each must display Appropriate Legal Notices; however, if the Program has interactive interfaces that do not display Appropriate Legal Notices, your work need not make them do so.

A compilation of a covered work with other separate and independent works, which are not by their nature extensions of the covered work, and which are not combined with it such as to form a larger program, in or on a volume of a storage or distribution medium, is called an “aggregate” if the compilation and its resulting copyright are not used to limit the access or legal rights of the compilation's users beyond what the individual works permit. Inclusion of a covered work in an aggregate does not cause this License to apply to the other parts of the aggregate.

6. Conveying Non-Source Forms.
You may convey a covered work in object code form under the terms of sections 4 and 5, provided that you also convey the machine-readable Corresponding Source under the terms of this License, in one of these ways:
a) Convey the object code in, or embodied in, a physical product (including a physical distribution medium), accompanied by the Corresponding Source fixed on a durable physical medium customarily used for software interchange.
b) Convey the object code in, or embodied in, a physical product (including a physical distribution medium), accompanied by a written offer, valid for at least three years and valid for as long as you offer spare parts or customer support for that product model, to give anyone who possesses the object code either (1) a copy of the Corresponding Source for all the software in the product that is covered by this License, on a durable physical medium customarily used for software interchange, for a price no more than your reasonable cost of physically performing this conveying of source, or (2) access to copy the Corresponding Source from a network server at no charge.
c) Convey individual copies of the object code with a copy of the written offer to provide the Corresponding Source. This alternative is allowed only occasionally and noncommercially, and only if you received the object code with such an offer, in accord with subsection 6b.
d) Convey the object code by offering access from a designated place (gratis or for a charge), and offer equivalent access to the Corresponding Source in the same way through the same place at no further charge. You need not require recipients to copy the Corresponding Source along with the object code. If the place to copy the object code is a network server, the Corresponding Source may be on a different server (operated by you or a third party) that supports equivalent copying facilities, provided you maintain clear directions next to the object code saying where to find the Corresponding Source. Regardless of what server hosts the Corresponding Source, you remain obligated to ensure that it is available for as long as needed to satisfy these requirements.
e) Convey the object code using peer-to-peer transmission, provided you inform other peers where the object code and Corresponding Source of the work are being offered to the general public at no charge under subsection 6d.

A separable portion of the object code, whose source code is excluded from the Corresponding Source as a System Library, need not be included in conveying the object code work.
A “User Product” is either (1) a “consumer product”, which means any tangible personal property which is normally used for personal, family, or household purposes, or (2) anything designed or sold for incorporation into a dwelling. In determining whether a product is a consumer product, doubtful cases shall be resolved in favor of coverage. For a particular product received by a particular user, “normally used” refers to a typical or common use of that class of product, regardless of the status of the particular user or of the way in which the particular user actually uses, or expects or is expected to use, the product. A product is a consumer product regardless of whether the product has substantial commercial, industrial or non-consumer uses, unless such uses represent the only significant mode of use of the product.
“Installation Information” for a User Product means any methods, procedures, authorization keys, or other information required to install and execute modified versions of a covered work in that User Product from a modified version of its Corresponding Source. The information must suffice to ensure that the continued functioning of the modified object code is in no case prevented or interfered with solely because modification has been made.
If you convey an object code work under this section in, or with, or specifically for use in, a User Product, and the conveying occurs as part of a transaction in which the right of possession and use of the User Product is transferred to the recipient in perpetuity or for a fixed term (regardless of how the transaction is characterized), the Corresponding Source conveyed under this section must be accompanied by the Installation Information. But this requirement does not apply if neither you nor any third party retains the ability to install modified object code on the User Product (for example, the work has been installed in ROM).
The requirement to provide Installation Information does not include a requirement to continue to provide support service, warranty, or updates for a work that has been modified or installed by the recipient, or for the User Product in which it has been modified or installed. Access to a network may be denied when the modification itself materially and adversely affects the operation of the network or violates the rules and protocols for communication across the network.
Corresponding Source conveyed, and Installation Information provided, in accord with this section must be in a format that is publicly documented (and with an implementation available to the public in source code form), and must require no special password or key for unpacking, reading or copying.

7. Additional Terms.
“Additional permissions” are terms that supplement the terms of this License by making exceptions from one or more of its conditions. Additional permissions that are applicable to the entire Program shall be treated as though they were included in this License, to the extent that they are valid under applicable law. If additional permissions apply only to part of the Program, that part may be used separately under those permissions, but the entire Program remains governed by this License without regard to the additional permissions.
When you convey a copy of a covered work, you may at your option remove any additional permissions from that copy, or from any part of it. (Additional permissions may be written to require their own removal in certain cases when you modify the work.) You may place additional permissions on material, added by you to a covered work, for which you have or can give appropriate copyright permission.
Notwithstanding any other provision of this License, for material you add to a covered work, you may (if authorized by the copyright holders of that material) supplement the terms of this License with terms:

a) Disclaiming warranty or limiting liability differently from the terms of sections 15 and 16 of this License; or
b) Requiring preservation of specified reasonable legal notices or author attributions in that material or in the Appropriate Legal Notices displayed by works containing it; or
c) Prohibiting misrepresentation of the origin of that material, or requiring that modified versions of such material be marked in reasonable ways as different from the original version; or
d) Limiting the use for publicity purposes of names of licensors or authors of the material; or
e) Declining to grant rights under trademark law for use of some trade names, trademarks, or service marks; or
f) Requiring indemnification of licensors and authors of that material by anyone who conveys the material (or modified versions of it) with contractual assumptions of liability to the recipient, for any liability that these contractual assumptions directly impose on those licensors and authors.
All other non-permissive additional terms are considered “further restrictions” within the meaning of section 10. If the Program as you received it, or any part of it, contains a notice stating that it is governed by this License along with a term that is a further restriction, you may remove that term. If a license document contains a further restriction but permits relicensing or conveying under this License, you may add to a covered work material governed by the terms of that license document, provided that the further restriction does not survive such relicensing or conveying.

If you add terms to a covered work in accord with this section, you must place, in the relevant source files, a statement of the additional terms that apply to those files, or a notice indicating where to find the applicable terms.
Additional terms, permissive or non-permissive, may be stated in the form of a separately written license, or stated as exceptions; the above requirements apply either way.

8. Termination.
You may not propagate or modify a covered work except as expressly provided under this License. Any attempt otherwise to propagate or modify it is void, and will automatically terminate your rights under this License (including any patent licenses granted under the third paragraph of section 11).
However, if you cease all violation of this License, then your license from a particular copyright holder is reinstated (a) provisionally, unless and until the copyright holder explicitly and finally terminates your license, and (b) permanently, if the copyright holder fails to notify you of the violation by some reasonable means prior to 60 days after the cessation.
Moreover, your license from a particular copyright holder is reinstated permanently if the copyright holder notifies you of the violation by some reasonable means, this is the first time you have received notice of violation of this License (for any work) from that copyright holder, and you cure the violation prior to 30 days after your receipt of the notice.
Termination of your rights under this section does not terminate the licenses of parties who have received copies or rights from you under this License. If your rights have been terminated and not permanently reinstated, you do not qualify to receive new licenses for the same material under section 10.

9. Acceptance Not Required for Having Copies.
You are not required to accept this License in order to receive or run a copy of the Program. Ancillary propagation of a covered work occurring solely as a consequence of using peer-to-peer transmission to receive a copy likewise does not require acceptance. However, nothing other than this License grants you permission to propagate or modify any covered work. These actions infringe copyright if you do not accept this License. Therefore, by modifying or propagating a covered work, you indicate your acceptance of this License to do so.

10. Automatic Licensing of Downstream Recipients.
Each time you convey a covered work, the recipient automatically receives a license from the original licensors, to run, modify and propagate that work, subject to this License. You are not responsible for enforcing compliance by third parties with this License.
An “entity transaction” is a transaction transferring control of an organization, or substantially all assets of one, or subdividing an organization, or merging organizations. If propagation of a covered work results from an entity transaction, each party to that transaction who receives a copy of the work also receives whatever licenses to the work the party's predecessor in interest had or could give under the previous paragraph, plus a right to possession of the Corresponding Source of the work from the predecessor in interest, if the predecessor has it or can get it with reasonable efforts.
You may not impose any further restrictions on the exercise of the rights granted or affirmed under this License. For example, you may not impose a license fee, royalty, or other charge for exercise of rights granted under this License, and you may not initiate litigation (including a cross-claim or counterclaim in a lawsuit) alleging that any patent claim is infringed by making, using, selling, offering for sale, or importing the Program or any portion of it.

11. Patents.
A “contributor” is a copyright holder who authorizes use under this License of the Program or a work on which the Program is based. The work thus licensed is called the contributor's “contributor version”.
A contributor's “essential patent claims” are all patent claims owned or controlled by the contributor, whether already acquired or hereafter acquired, that would be infringed by some manner, permitted by this License, of making, using, or selling its contributor version, but do not include claims that would be infringed only as a consequence of further modification of the contributor version. For purposes of this definition, “control” includes the right to grant patent sublicenses in a manner consistent with the requirements of this License.
Each contributor grants you a non-exclusive, worldwide, royalty-free patent license under the contributor's essential patent claims, to make, use, sell, offer for sale, import and otherwise run, modify and propagate the contents of its contributor version.
In the following three paragraphs, a “patent license” is any express agreement or commitment, however denominated, not to enforce a patent (such as an express permission to practice a patent or covenant not to sue for patent infringement). To “grant” such a patent license to a party means to make such an agreement or commitment not to enforce a patent against the party.
If you convey a covered work, knowingly relying on a patent license, and the Corresponding Source of the work is not available for anyone to copy, free of charge and under the terms of this License, through a publicly available network server or other readily accessible means, then you must either (1) cause the Corresponding Source to be so available, or (2) arrange to deprive yourself of the benefit of the patent license for this particular work, or (3) arrange, in a manner consistent with the requirements of this License, to extend the patent license to downstream recipients. “Knowingly relying” means you have actual knowledge that, but for the patent license, your conveying the covered work in a country, or your recipient's use of the covered work in a country, would infringe one or more identifiable patents in that country that you have reason to believe are valid.
If, pursuant to or in connection with a single transaction or arrangement, you convey, or propagate by procuring conveyance of, a covered work, and grant a patent license to some of the parties receiving the covered work authorizing them to use, propagate, modify or convey a specific copy of the covered work, then the patent license you grant is automatically extended to all recipients of the covered work and works based on it.
A patent license is “discriminatory” if it does not include within the scope of its coverage, prohibits the exercise of, or is conditioned on the non-exercise of one or more of the rights that are specifically granted under this License. You may not convey a covered work if you are a party to an arrangement with a third party that is in the business of distributing software, under which you make payment to the third party based on the extent of your activity of conveying the work, and under which the third party grants, to any of the parties who would receive the covered work from you, a discriminatory patent license (a) in connection with copies of the covered work conveyed by you (or copies made from those copies), or (b) primarily for and in connection with specific products or compilations that contain the covered work, unless you entered into that arrangement, or that patent license was granted, prior to 28 March 2007.

Nothing in this License shall be construed as excluding or limiting any implied license or other defenses to infringement that may otherwise be available to you under applicable patent law.

12. No Surrender of Others' Freedom.
If conditions are imposed on you (whether by court order, agreement or otherwise) that contradict the conditions of this License, they do not excuse you from the conditions of this License. If you cannot convey a covered work so as to satisfy simultaneously your obligations under this License and any other pertinent obligations, then as a consequence you may not convey it at all. For example, if you agree to terms that obligate you to collect a royalty for further conveying from those to whom you convey the Program, the only way you could satisfy both those terms and this License would be to refrain entirely from conveying the Program.

13. Use with the GNU Affero General Public License.
Notwithstanding any other provision of this License, you have permission to link or combine any covered work with a work licensed under version 3 of the GNU Affero General Public License into a single combined work, and to convey the resulting work. The terms of this License will continue to apply to the part which is the covered work, but the special requirements of the GNU Affero General Public License, section 13, concerning interaction through a network will apply to the combination as such.

14. Revised Versions of this License.
The Free Software Foundation may publish revised and/or new versions of the GNU General Public License from time to time. Such new versions will be similar in spirit to the present version, but may differ in detail to address new problems or concerns.
Each version is given a distinguishing version number. If the Program specifies that a certain numbered version of the GNU General Public License “or any later version” applies to it, you have the option of following the terms and conditions either of that numbered version or of any later version published by the Free Software Foundation. If the Program does not specify a version number of the GNU General Public License, you may choose any version ever published by the Free Software Foundation.
If the Program specifies that a proxy can decide which future versions of the GNU General Public License can be used, that proxy's public statement of acceptance of a version permanently authorizes you to choose that version for the Program.
Later license versions may give you additional or different permissions. However, no additional obligations are imposed on any author or copyright holder as a result of your choosing to follow a later version.

15. Disclaimer of Warranty.
THERE IS NO WARRANTY FOR THE PROGRAM, TO THE EXTENT PERMITTED BY APPLICABLE LAW. EXCEPT WHEN OTHERWISE STATED IN WRITING THE COPYRIGHT HOLDERS AND/OR OTHER PARTIES PROVIDE THE PROGRAM “AS IS” WITHOUT WARRANTY OF ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE ENTIRE RISK AS TO THE QUALITY AND PERFORMANCE OF THE PROGRAM IS WITH YOU. SHOULD THE PROGRAM PROVE DEFECTIVE, YOU ASSUME THE COST OF ALL NECESSARY SERVICING, REPAIR OR CORRECTION.

16. Limitation of Liability.
IN NO EVENT UNLESS REQUIRED BY APPLICABLE LAW OR AGREED TO IN WRITING WILL ANY COPYRIGHT HOLDER, OR ANY OTHER PARTY WHO MODIFIES AND/OR CONVEYS THE PROGRAM AS PERMITTED ABOVE, BE LIABLE TO YOU FOR DAMAGES, INCLUDING ANY GENERAL, SPECIAL, INCIDENTAL OR CONSEQUENTIAL DAMAGES ARISING OUT OF THE USE OR INABILITY TO USE THE PROGRAM (INCLUDING BUT NOT LIMITED TO LOSS OF DATA OR DATA BEING RENDERED INACCURATE OR LOSSES SUSTAINED BY YOU OR THIRD PARTIES OR A FAILURE OF THE PROGRAM TO OPERATE WITH ANY OTHER PROGRAMS), EVEN IF SUCH HOLDER OR OTHER PARTY HAS BEEN ADVISED OF THE POSSIBILITY OF SUCH DAMAGES.

17. Interpretation of Sections 15 and 16.
If the disclaimer of warranty and limitation of liability provided above cannot be given local legal effect according to their terms, reviewing courts shall apply local law that most closely approximates an absolute waiver of all civil liability in connection with the Program, unless a warranty or assumption of liability accompanies a copy of the Program in return for a fee.

END OF TERMS AND CONDITIONS"""

logging.basicConfig(filename=os.path.expanduser('~/.nvmeshcli.log'),
                    format="%(asctime)s - %(levelname)-8s - %(message)s",
                    level=logging.DEBUG)


class ArgsUsageOutputFormatter(argparse.HelpFormatter):
    def _format_usage(self, usage, actions, groups, prefix):
        if prefix is None:
            prefix = 'Usage: '
        if usage is not None:
            usage = usage % dict(prog=self._prog)
        elif usage is None and not actions:
            usage = '%(prog)s' % dict(prog=self._prog)
        elif usage is None:
            prog = '%(prog)s' % dict(prog=self._prog)
            action_usage = self._format_actions_usage(actions, groups)
            usage = ' '.join([s for s in [prog, action_usage] if s])
        return '%s%s\n\n' % (prefix, usage)


class OutputFormatter:
    def __init__(self):
        self.text = None

    @staticmethod
    def print_green(text):
        print('\033[92m' + text + '\033[0m')

    @staticmethod
    def print_yellow(text):
        print('\033[33m' + text + '\033[0m')

    @staticmethod
    def print_red(text):
        print('\033[31m' + text + '\033[0m')

    @staticmethod
    def green(text):
        return '\033[92m' + text + '\033[0m'

    @staticmethod
    def yellow(text):
        return '\033[33m' + text + '\033[0m'

    @staticmethod
    def red(text):
        return '\033[31m' + text + '\033[0m'

    @staticmethod
    def bold(text):
        return '\033[1m' + text + '\033[0m'

    @staticmethod
    def bold_underline(text):
        return '\033[1m\033[4m' + text + '\033[0m'

    @staticmethod
    def echo(host, text):
        print("[ " + host.strip() + " ]\t.\t.\t." + text)
        return

    @staticmethod
    def print_tsv(content):
        output = []
        for line in content:
            output_line = "\t".join(str(item) for item in line)
            output.append(output_line)
        return "\n".join(output)

    @staticmethod
    def print_json(content):
        return json.dumps(content, indent=2)

    @staticmethod
    def add_line_prefix(prefix, text, short):
        if short:
            text_lines = [' '.join([prefix.split('.')[0], line]) for line in text.splitlines()]
        else:
            text_lines = [' '.join([prefix, line]) for line in text.splitlines()]
        return '\n'.join(text_lines)


class Hosts:
    def __init__(self):
        self.host_list = []
        self.host_file = os.path.expanduser('~/.nvmesh_hosts')
        self.test_host_connection_test_result = None
        self.formatter = OutputFormatter()
        self.host_delete_list = []

    def manage_hosts(self, action, hosts_list, silent):
        if action == "add":
            open(self.host_file, 'a').write(('\n'.join(hosts_list) + '\n'))
        elif action == "get":
            if os.path.isfile(self.host_file):
                output = []
                self.host_list = open(self.host_file, 'r').readlines()
                for host in self.host_list:
                    output.append(host.strip())
                return output
            else:
                if silent:
                    return None
                else:
                    return [self.formatter.yellow(
                        "No hosts defined! Use 'add hosts' to add hosts to your shell environment.")]
        elif action == "delete":
            tmp_host_list = []
            if os.path.isfile(self.host_file):
                for line in open(self.host_file, 'r').readlines():
                    tmp_host_list.append(line.strip())
                for host in hosts_list:
                    tmp_host_list.remove(host.strip())
                open(self.host_file, 'w').write(('\n'.join(tmp_host_list) + '\n'))
            else:
                return [self.formatter.yellow(
                    "No hosts defined! Use 'add hosts' to add hosts to your shell environment.")]


class ManagementServer:
    def __init__(self):
        self.server = None
        self.server_list = []
        self.server_file = os.path.expanduser('~/.nvmesh_manager')

    def get_management_server_list(self):
        if os.path.isfile(self.server_file):
            self.server = [server.strip() for server in open(self.server_file, 'r').readlines()]
            return sorted(self.server)
        else:
            formatter.print_yellow("No API management server defined yet!")
            server_list = raw_input(
                "Provide a space separated list, min. one, of the NVMesh manager server names: ").split(" ")
            self.save_management_server(server_list)
            return sorted(server_list)

    def save_management_server(self, server_list):
        open(self.server_file, 'w').write("\n".join(server_list))
        return


class UserCredentials:
    def __init__(self):
        self.SSH_user_name = None
        self.SSH_password = None
        self.API_user_name = None
        self.API_password = None
        self.SSH_sudo = None
        self.SSH_secrets_file = os.path.expanduser('~/.nvmesh_shell_secrets')
        self.API_secrets_file = os.path.expanduser('~/.nvmesh_api_secrets')
        self.SSH_sudo_file = os.path.expanduser('~/.nvmesh_shell_sudo')
        self.SSH_secrets = None
        self.API_secrets = None

    def save_ssh_user(self):
        if self.SSH_user_name is None or self.SSH_password is None:
            formatter.print_red('Cannot store SSH user credentials! '
                                'Both, user name and password need to be set/defined!')
        else:
            secrets = open(self.SSH_secrets_file, 'w')
            secrets.write(' '.join([self.SSH_user_name, base64.b64encode(self.SSH_password)]))
            secrets.close()

    def save_api_user(self):
        if self.API_user_name is None or self.API_password is None:
            formatter.print_red('Cannot store API user credentials! '
                                'Both, user name and password need to be set/defined!')
        else:
            secrets = open(self.API_secrets_file, 'w')
            secrets.write(' '.join([self.API_user_name, base64.b64encode(self.API_password)]))
            secrets.close()

    def save_ssh_sudo(self, is_sudo):
        sudo = open(self.SSH_sudo_file, 'w')
        sudo.write(str(is_sudo))
        sudo.close()

    def get_ssh_user(self):
        try:
            self.SSH_secrets = open(self.SSH_secrets_file, 'r').read().split(' ')
            self.SSH_sudo = open(self.SSH_sudo_file, 'r').read().strip()
        except Exception, e:
            logging.critical(e.message)
            formatter.print_red(e.message)
            pass
        if self.SSH_secrets is None:
            formatter.print_yellow("SSH user credentials not set yet!")
            self.SSH_user_name = raw_input("Provide the root level SSH user name: ")
            self.SSH_password = getpass.getpass("Please provide the SSH password: ")
            self.save_ssh_user()
            self.get_ssh_user()
            return self.SSH_user_name
        else:
            self.SSH_user_name = self.SSH_secrets[0]
            self.SSH_password = base64.b64decode(self.SSH_secrets[1])
            return self.SSH_user_name, self.SSH_password

    def get_api_user(self):
        try:
            self.API_secrets = open(self.API_secrets_file, 'r').read().split(' ')
        except Exception, e:
            logging.critical(e.message)
            formatter.print_red(e.message)
            pass
        if self.API_secrets is None:
            formatter.print_yellow("API user credentials not set yet!")
            self.API_user_name = raw_input("Provide the root level API user name: ")
            self.API_password = getpass.getpass("Please provide the API password: ")
            self.save_api_user()
            self.get_api_user()
            return self.API_user_name
        else:
            self.API_user_name = self.API_secrets[0]
            self.API_password = base64.b64decode(self.API_secrets[1])
            return self.API_user_name


class SSHRemoteOperations:
    def __init__(self):
        self.remote_path = "/tmp/nvmesh_diag/"
        self.local_path = os.path.abspath("nvmesh_diag/")
        self.formatter = OutputFormatter
        self.file_list = []
        self.ssh_port = 22
        self.ssh = paramiko.SSHClient()
        self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.sftp = self.ssh.open_sftp
        self.remote_return_code = None
        self.remote_stdout = None
        self.remote_command_return = None
        self.remote_command_error = None
        self.ssh_user_name = user.get_ssh_user()[0]
        self.ssh_password = user.get_ssh_user()[1]

    def test_ssh_connection(self, host_list):
        if host_list is None:
            host_list = Hosts().manage_hosts('get', None, True)
        if host_list is None:
            host_list = get_client_list(False)
            host_list.extend(get_target_list(short=True))
            host_list.extend(get_manager_list(short=True))
            host_list = set(host_list)
        for host in host_list:
            try:
                self.ssh.connect(
                    host, username=user.SSH_user_name, password=user.SSH_password, timeout=5, port=self.ssh_port)
                self.ssh.close()
                print(" ".join(['Connection to %s' % host, formatter.green('OK')]))
            except Exception, e:
                print(" ".join(['Connection to %s' % host, formatter.red('Failed:'), e.message]))
                self.ssh.close()
        return

    def transfer_files(self, host, list_of_files):
        try:
            self.ssh.connect(
                host, username=user.SSH_user_name, password=user.SSH_password, timeout=5, port=self.ssh_port)
            self.sftp = self.ssh.open_sftp()
            try:
                self.sftp.chdir(self.remote_path)
            except IOError:
                self.sftp.mkdir(self.remote_path)
            for file_to_transfer in list_of_files:
                self.sftp.put(self.local_path + "/" + file_to_transfer, self.remote_path + "/" + file_to_transfer)
            self.sftp.close()
            self.ssh.close()
            return formatter.green("File transfer to host %s OK" % host)
        except Exception, e:
            cli_exit.error = True
            return formatter.red("File transfer to %s Failed! " % host + e.message)

    def return_remote_command_std_output(self, host, remote_command):
        try:
            self.ssh.connect(host, username=self.ssh_user_name, password=self.ssh_password, timeout=5,
                             port=self.ssh_port)
            if user.SSH_sudo.lower() == 'true':
                remote_command = " ".join(["sudo -S -p ''", remote_command])
            stdin, stdout, stderr = self.ssh.exec_command(remote_command)
            if user.SSH_sudo.lower() == 'true':
                stdin.write(user.SSH_password + "\n")
                stdin.flush()
            self.remote_command_return = stdout.channel.recv_exit_status(), stdout.read().strip(), stderr.read().strip()
            if self.remote_command_return[0] == 0:
                return self.remote_command_return[0], self.remote_command_return[1]
            elif self.remote_command_return[0] == 3:
                cli_exit.error = True
                return "Service not running."
            elif self.remote_command_return[0] == 127:
                cli_exit.error = True
                return self.remote_command_return[0], remote_command + " not found or not installed!"
            else:
                cli_exit.error = True
                return self.remote_command_return[0], " ".join([remote_command, self.remote_command_return[1]])
        except Exception, e:
            cli_exit.error = True
            logging.critical(e.message)
            print formatter.red("Couldn't execute command %s on %s! %s" % (remote_command, host, e.message))

    def execute_remote_command(self, host, remote_command):
        try:
            self.ssh.connect(host.strip(), username=user.SSH_user_name, password=user.SSH_password, timeout=5,
                             port=self.ssh_port)
            if user.SSH_sudo:
                remote_command = " ".join(["sudo -S -p ''", remote_command])
            stdin, stdout, stderr = self.ssh.exec_command(remote_command)
            if user.SSH_sudo:
                stdin.write(user.SSH_password + "\n")
                stdin.flush()
            return stdout.channel.recv_exit_status(), "Success - OK"
        except Exception, e:
            logging.critical(e.message)
            cli_exit.error = True
            print formatter.print_red("Couldn't execute command %s on %s!" % (remote_command, host))
            return

    def check_if_service_is_running(self, host, service):
        try:
            cmd_output = self.execute_remote_command(host, "/opt/NVMesh/%s/services/%s status" % (service, service))
            if cmd_output[0] == 0:
                return True
            elif cmd_output[0] == 3:
                cli_exit.error = True
                return False
            else:
                cli_exit.error = True
                return None
        except Exception, e:
            cli_exit.error = True
            print formatter.print_red("Couldn't verify service %s on %s !" % (service, host) + e.message)


class Api:
    def __init__(self):
        self.protocol = 'https'
        self.server = None
        self.port = '4000'
        self.user_name = None
        self.password = None
        self.endpoint = None
        self.payload = None
        self.session = requests.session()
        self.response = None
        self.session.verify = False
        self.err = None
        self.action = None
        self.timeout = 10

    def execute_api_call(self):
        try:
            if self.action == "post":
                logging.debug(
                    "API action: POST %s://%s:%s%s" % (self.protocol, self.server, self.port, self.endpoint))
                logging.debug("API payload: %s" % self.payload if '/login' not in self.endpoint else 'login')
                if self.payload:
                    self.response = self.session.post(
                        '%s://%s:%s%s' % (self.protocol, self.server, self.port, self.endpoint), json=self.payload,
                        timeout=self.timeout, verify=False)
                else:
                    self.response = self.session.post(
                        '%s://%s:%s%s' % (self.protocol, self.server, self.port, self.endpoint), timeout=self.timeout)
                    logging.debug("API response: %s" % self.response)
                    logging.debug("API response content is: %s"
                                  % self.response.content if '/login' not in self.endpoint else 'login')
                return self.response.content
            elif self.action == "get":
                logging.debug("API action: GET %s://%s:%s%s" % (self.protocol, self.server, self.port, self.endpoint))
                self.response = self.session.get(
                    "%s://%s:%s%s" % (self.protocol, self.server, self.port, self.endpoint), timeout=self.timeout,
                    verify=False)
                logging.debug("API response status code: %s" % self.response)
                logging.debug("API response content is: %s" % self.response.content)
                return self.response.content
        except Exception, e:
            cli_exit.error = True
            logging.critical(e.message)
            print(formatter.red("Error: " + e.message))

    def login(self):
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.action = "post"
        self.endpoint = '/login'
        self.payload = {
            "username": self.user_name,
            "password": self.password
        }
        return self.execute_api_call()

    def get_cluster(self):
        self.endpoint = '/status'
        self.action = "get"
        return self.execute_api_call()

    def get_space_allocation(self):
        self.endpoint = '/getSpaceAllocation'
        self.action = "get"
        return self.execute_api_call()

    def get_servers(self):
        self.endpoint = '/servers/all/%s/%s' % (0, 0)
        self.action = "get"
        return self.execute_api_call()

    def get_clients(self):
        self.endpoint = '/clients/all/%s/%s' % (0, 0)
        self.action = "get"
        return self.execute_api_call()

    def get_volumes(self):
        self.endpoint = '/volumes/all/%s/%s' % (0, 0)
        self.action = "get"
        return self.execute_api_call()

    def get_volume(self, volume_id):
        self.endpoint = '/volumes/all/%s/%s?filter={"_id":"%s"}&sort={}' % (0, 0, volume_id)
        self.action = "get"
        return self.execute_api_call()

    def get_cluster_status(self):
        self.endpoint = '/status'
        self.action = "get"
        return self.execute_api_call()

    def get_logs(self, all_logs):
        if all_logs:
            self.endpoint = '/logs/all/0/0?filter={}&sort={"timestamp":-1}'
        else:
            self.endpoint = '/logs/alerts/0/0?filter={}&sort={"timestamp":-1}'
        self.action = "get"
        return self.execute_api_call()

    def get_vpgs(self):
        self.endpoint = '/volumeProvisioningGroups/all'
        self.action = "get"
        return self.execute_api_call()

    def get_disk_classes(self):
        self.endpoint = '/diskClasses/all'
        self.action = "get"
        return self.execute_api_call()

    def get_drive_class(self, name):
        self.endpoint = '/diskClasses/all?filter={"_id": "%s"}&sort={}' % name
        self.action = "get"
        return self.execute_api_call()

    def update_drive_class(self, payload):
        self.endpoint = '/diskClasses/update'
        self.action = "post"
        self.payload = payload
        return self.execute_api_call()

    def get_disk_models(self):
        self.endpoint = '/disks/models'
        self.action = "get"
        return self.execute_api_call()

    def get_disk_by_model(self, model):
        self.endpoint = '/disks/disksByModel/%s' % model
        self.action = "get"
        return self.execute_api_call()

    def get_target_classes(self):
        self.endpoint = '/serverClasses/all'
        self.action = "get"
        return self.execute_api_call()

    def get_target_class(self, name):
        self.endpoint = '/serverClasses/all?filter={"_id": "%s"}&sort={}' % name
        self.action = "get"
        return self.execute_api_call()

    def update_target_class(self, payload):
        self.endpoint = '/serverClasses/update'
        self.action = "post"
        self.payload = payload
        return self.execute_api_call()

    def get_server_by_id(self, server):
        self.endpoint = '/servers/api/%s' % server
        self.action = "get"
        return self.execute_api_call()

    def target_cluster_shutdown(self, payload):
        self.endpoint = '/servers/setBatchControlJobs'
        self.action = "post"
        self.payload = payload
        return self.execute_api_call()

    def manage_volume(self, payload):
        self.payload = payload
        self.endpoint = '/volumes/save'
        self.action = "post"
        return self.execute_api_call()

    def manage_vpg(self, action, payload):
        self.payload = payload
        self.action = "post"
        if action == 'save':
            self.endpoint = '/volumeProvisioningGroups/save'
        elif action == 'delete':
            self.endpoint = '/volumeProvisioningGroups/delete'
        return self.execute_api_call()

    def set_control_jobs(self, payload):
        self.payload = payload
        self.endpoint = '/clients/setControlJobs'
        self.action = "post"
        return self.execute_api_call()

    def manage_drive_class(self, action, payload):
        self.payload = payload
        self.endpoint = '/diskClasses/%s' % action
        self.action = "post"
        return self.execute_api_call()

    def manage_target_class(self, action, payload):
        self.payload = payload
        self.endpoint = '/serverClasses/%s' % action
        self.action = "post"
        return self.execute_api_call()

    def get_managers(self):
        self.endpoint = '/managementCluster/all/0/0'
        self.action = 'get'
        return self.execute_api_call()

    def evict_drive(self, payload):
        self.payload = payload
        self.endpoint = '/disks/evictDiskByDiskIds'
        self.action = 'post'
        return self.execute_api_call()

    def delete_drive(self, payload):
        self.payload = payload
        self.endpoint = '/disks/delete'
        self.action = 'post'
        return self.execute_api_call()

    def delete_nic(self, payload):
        self.payload = payload
        self.endpoint = '/servers/deleteNIC'
        self.action = 'post'
        return self.execute_api_call()

    def format_drive(self, payload):
        self.payload = payload
        self.endpoint = '/disks/formatDiskByDiskIds'
        self.action = 'post'
        return self.execute_api_call()


class Exit:
    def __init__(self):
        self.error = None
        self.is_interactive = None

    def validate_exit(self):
        if self.is_interactive is False:
            if self.error:
                sys.exit(1)


formatter = OutputFormatter()
user = UserCredentials()
nvmesh = Api()
mgmt = ManagementServer()
hosts = Hosts()
cli_exit = Exit()


class NvmeshShell(Cmd):

    def __init__(self):
        Cmd.__init__(self, use_ipython=True)
        self.hidden_commands = ['py', 'ipy', 'pyscript', '_relative_load', 'eof', 'eos', 'exit']

    prompt = "\033[1;34mnvmesh #\033[0m "
    show_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    show_parser.add_argument('nvmesh_object', choices=['cluster', 'target', 'client', 'volume', 'drive', 'manager',
                                                       'sshuser', 'apiuser', 'vpg', 'driveclass', 'targetclass',
                                                       'host', 'log', 'drivemodel', 'version', 'license'],
                             help='The NVMesh object you want to list or view.')
    show_parser.add_argument('-a', '--all', required=False, action='store_const', const=True, default=False,
                             help='Show all logs. Per default only alerts are shown.')
    show_parser.add_argument('-C', '--Class', nargs='+', required=False,
                             help='A single or a space separated list of NVMesh drives or target classes.')
    show_parser.add_argument('-d', '--detail', required=False, action='store_const', const=True,
                             help='Show more details.')
    show_parser.add_argument('-l', '--layout', required=False, action='store_const', const=True,
                             help='Show the volume layout details. To be used together with the "-d" switch.')
    show_parser.add_argument('-j', '--json', required=False, action='store_const', const=True,
                             help='Format output as JSON.')
    show_parser.add_argument('-s', '--server', nargs='+', required=False,
                             help='Space separated list or single server.')
    show_parser.add_argument('-S', '--short-name', required=False, action='store_const', const=True,
                             help='Show short hostnames.')
    show_parser.add_argument('-t', '--tsv', required=False, action='store_const', const=True,
                             help='Format output as tabulator separated values.')
    show_parser.add_argument('-v', '--volume', nargs='+', required=False,
                             help='View a single NVMesh volume or a list of volumes.')
    show_parser.add_argument('-p', '--vpg', nargs='+', required=False,
                             help='View a single or a list of NVMesh volume provisioning groups.')

    @with_argparser(show_parser)
    @with_category("NVMesh Resource Management")
    def do_show(self, args):
        """List and view specific Nvmesh objects and its properties. The 'list sub-command allows output in a table,
        tabulator separated value or JSON format. E.g 'show target' will list all targets. In case you want to see the
        properties of only one or just a few you need to use the '-s' or '--server' option to specify single or a list
        of servers/targets. E.g. 'list targets -s target1 target2'"""
        user.get_api_user()
        if args.nvmesh_object == 'target':
            self.poutput(show_target(args.detail,
                                     args.tsv,
                                     args.json,
                                     args.server,
                                     args.short_name))
        elif args.nvmesh_object == 'client':
            self.poutput(show_clients(args.tsv,
                                      args.json,
                                      args.server,
                                      args.short_name))
        elif args.nvmesh_object == 'volume':
            self.poutput(show_volumes(args.detail,
                                      args.tsv,
                                      args.json,
                                      args.volume,
                                      args.short_name,
                                      args.layout))
        elif args.nvmesh_object == 'sshuser':
            self.poutput(user.get_ssh_user()[0])
        elif args.nvmesh_object == 'apiuser':
            self.poutput(user.get_api_user())
        elif args.nvmesh_object == 'manager':
            self.poutput(show_manager())
        elif args.nvmesh_object == 'cluster':
            self.poutput(show_cluster(args.tsv,
                                      args.json))
        elif args.nvmesh_object == 'vpg':
            self.poutput(show_vpgs(args.tsv,
                                   args.json,
                                   args.vpg))
        elif args.nvmesh_object == 'driveclass':
            self.poutput(show_drive_classes(args.detail,
                                            args.tsv,
                                            args.json,
                                            args.Class))
        elif args.nvmesh_object == 'targetclass':
            self.poutput(show_target_classes(args.tsv,
                                             args.json,
                                             args.Class))
        elif args.nvmesh_object == 'host':
            self.poutput("\n".join(hosts.manage_hosts("get", None, False)))
        elif args.nvmesh_object == 'log':
            self.ppaged(show_logs(args.all))
        elif args.nvmesh_object == 'drive':
            self.poutput(show_drives(args.detail,
                                     args.server,
                                     args.tsv))
        elif args.nvmesh_object == 'drivemodel':
            self.poutput(show_drive_models(args.detail))
        elif args.nvmesh_object == 'version':
            self.poutput(": ".join(["Nvmesh CLI version", __version__]))
        elif args.nvmesh_object == 'license':
            self.ppaged(__license__)
        cli_exit.validate_exit()

    add_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    add_parser.add_argument('nvmesh_object', choices=['host', 'volume', 'driveclass', 'targetclass'],
                            help='Add hosts to this shell environment or '
                                 'add/create new NVMesh volumes or drive classes.')
    add_parser.add_argument('-a', '--autocreate', required=False, action='store_const', const=True, default=False,
                            help='Create the drive classes automatically grouped by the available drive models.')
    add_parser.add_argument('-r', '--raid_level', nargs=1, required=False,
                            help='The RAID level of the volume. Options: lvm, 0, 1, 10')
    add_parser.add_argument('-v', '--vpg', nargs=1, required=False,
                            help='Optional - The volume provisioning group to use.')
    add_parser.add_argument('-o', '--domain', nargs=1, required=False,
                            help='Awareness domain information to use for new volume/s or a VPG.')
    add_parser.add_argument('-D', '--description', nargs=1, required=False,
                            help='Optional - Volume description')
    add_parser.add_argument('-l', '--limit-by-disk', nargs='+', required=False,
                            help='Optional - Limit volume allocation to specific drives.')
    limit_target_server_group = add_parser.add_mutually_exclusive_group()
    limit_target_server_group.add_argument('-L', '--limit-by-target', nargs='+', required=False,
                                           help='Optional - Limit volume allocation to specific target nodes.')
    drive_file_group = add_parser.add_mutually_exclusive_group()
    drive_file_group.add_argument('-m', '--drive', nargs='+', required=False,
                                  help='Drive/media information. Needs to include the drive ID/serial and the target'
                                       'node/server name in the format driveId:targetName'
                                       'Example: -m "Example: 174019659DA4.1:test.lab"')
    drive_file_group.add_argument('-f', '--file', nargs=1, required=False,
                                  help='Path to the file containing the driveId:targetName information. '
                                       'Needs to'
                                       'Example: -f "/path/to/file". This argument is not allowed together with the -m '
                                       'argument')
    add_parser.add_argument('-M', '--model', nargs=1, required=False,
                            help='Drive model information for the new drive class. '
                                 'Note: Must be the exactly the same model designator as when running the'
                                 '"show drivemodel -d" or "show drive -d" command!')
    add_parser.add_argument('-n', '--name', nargs=1, required=False,
                            help='Name of the volume, must be unique, will be the ID of the volume.')
    add_parser.add_argument('-O', '--classdomain', nargs='+', required=False,
                            help="Awareness domain/s information of the target or drive class. "
                                 "A domain has a scope and identifier component. "
                                 "You must provide both components for each domain to be used/created."
                                 "-O scope:Rack&identifier:A "
                                 "or in case you want to use more than one domain descriptor:"
                                 "-O scope:Rack&identifier:A scope:Datacenter&identifier:DRsite")
    add_parser.add_argument('-P', '--parity', nargs=1, required=False,
                            help='Parity configuration. Required for Erasure Coding NVMesh volumes. Example: "8+2" '
                                 'which equals to 8 data blocks + 2 parity blocks')
    add_parser.add_argument('-R', '--node-redundancy', nargs=1, required=False,
                            help='NVMesh Target node redundancy configuration. Required for Erasure Coding NVMesh '
                                 'volumes. NVMesh supports three target node redundancy levels, aka. protection levels.'
                                 '0 = no separation or redundancy on the node level; 1 = N+1 node redundancy or '
                                 'minimal separation; 2 = N+2 redundancy or maximal separation. Chose between 0, 1, '
                                 'or 2.')
    add_parser.add_argument('-c', '--count', nargs=1, required=False,
                            help='Number of volumes to create and add. 100 Max.')
    add_parser.add_argument('-t', '--target-class', nargs='+', required=False,
                            help='Limit volume allocation to specific target classes.')
    add_parser.add_argument('-d', '--drive-class', nargs='+', required=False,
                            help='Limit volume allocation to specific drive classes.')
    add_parser.add_argument('-w', '--stripe-width', nargs=1, required=False,
                            help='Number of disks to use. Required for R0 and R10.')
    limit_target_server_group.add_argument('-s', '--server', nargs='+', required=False,
                                           help='Specify a single server or a space separated list of servers.')
    add_parser.add_argument('-S', '--size', nargs=1, required=False,
                            help='Specify the size of the new volume. The volumes size value is base*2/binary. '
                                 'Example: -S 12GB or 12GiB will create a volume with a size of 12884901888 bytes.'
                                 'Some valid input formats samples: xGB, x GB, x gigabyte, x GiB or xG')

    @with_argparser(add_parser)
    @with_category("NVMesh Resource Management")
    def do_add(self, args):
        """The 'add' sub-command will let you add nvmesh objects to your cluster. E.g. 'add host' will add host
        entries to your nvmeshcli environment while 'add volume' will create and add a new volume to the NVMesh
        cluster."""
        action = "add"
        if args.nvmesh_object == 'host':
            hosts.manage_hosts(action,
                               args.server,
                               False)
        elif args.nvmesh_object == 'driveclass':
            if args.autocreate:
                self.poutput(manage_drive_class("autocreate",
                                                None,
                                                None,
                                                None,
                                                None,
                                                None,
                                                None,
                                                None))
            else:
                if args.name is None:
                    print formatter.yellow(
                        "Drive class name missing! Use the -n argument to provide a name.")
                    return
                if not args.model:
                    print formatter.yellow(
                        "No drive model information specified. Use the -M argument to provide the drive model "
                        "information.")
                    return
                self.poutput(manage_drive_class("save",
                                                None,
                                                args.drive,
                                                args.model,
                                                args.name,
                                                args.description,
                                                args.classdomain,
                                                args.file))
        elif args.nvmesh_object == 'targetclass':
            if args.autocreate:
                self.poutput(manage_target_class("autocreate",
                                                 None,
                                                 None,
                                                 None,
                                                 None,
                                                 None))
            else:
                if args.name is None:
                    print formatter.yellow(
                        "Target class name missing! Use the -n argument to provide a name.")
                    return
                if not args.server:
                    print formatter.yellow(
                        "No target servers specified. use the -s argument to provide a space separated list of targets"
                        "to be used. At least one target must be defined.")
                    return
                self.poutput(manage_target_class("save",
                                                 None,
                                                 args.name[0],
                                                 args.server,
                                                 args.description,
                                                 parse_domain_args(args.classdomain)))
        elif args.nvmesh_object == 'volume':

            if args.limit_by_target:
                for server in args.limit_by_target:
                    if not bool(re.match(
                            "^(([a-zA-Z]|[a-zA-Z][a-zA-Z0-9\-]*[a-zA-Z0-9])\.)*([A-Za-z]|[A-Za-z][A-Za-z0-9\-]*[A-Za-z0-9])$",
                            server)):
                        print(formatter.yellow(
                            "Provided hostname " + server + " is not a valid target/server name! "
                                                            "If you try to provide a list of multiple servers, please "
                                                            "use a space separated list of server names!"))
                        return

            if args.name is None:
                print(formatter.yellow(
                    "Volume name missing! Use the -n argument to provide a volume name"))
                return
            if args.size is None:
                print(formatter.yellow(
                    "Size/capacity information is missing! Use the -S argument to provide the volume size."))
                return
            if args.raid_level is None and args.vpg is None:
                print(formatter.yellow(
                    "Raid level information missing! Use the -r argument to set the raid level."))
                return
            if args.raid_level[0] == 'ec':
                if args.parity is None:
                    print(formatter.yellow(
                        "Erasure coding parity information missing! Use the -P argument to set the parity "
                        "configuration."))
                    return
                if args.node_redundancy is None:
                    print(formatter.yellow(
                        "Erasure coding node redundancy aka. protection level information missing! Use the -R argument "
                        "to set the node redundancy configuration."))
                    return
            if args.vpg is None:
                if '0' in args.raid_level[0] and args.stripe_width is None:
                    print(formatter.yellow(
                        "Stripe width information missing! Use the -w argument to set the stripe width."))
                    return
            if args.count is not None:
                if int(args.count[0]) > 100:
                    self.poutput(formatter.yellow("Count too high! The max is 100."))
                    return
                else:
                    count = 1
                    while count <= int(args.count[0]):
                        name = "".join([args.name[0], "%03d" % (count,)])
                        count = count + 1
                        self.poutput(manage_volume('create',
                                                   name,
                                                   args.size,
                                                   args.description,
                                                   args.drive_class,
                                                   args.target_class,
                                                   args.limit_by_target,
                                                   args.limit_by_disk,
                                                   args.domain,
                                                   args.raid_level,
                                                   args.stripe_width,
                                                   args.vpg,
                                                   None,
                                                   args.parity,
                                                   args.node_redundancy))
            else:
                self.poutput(manage_volume('create',
                                           args.name[0],
                                           args.size,
                                           args.description,
                                           args.drive_class,
                                           args.target_class,
                                           args.limit_by_target,
                                           args.limit_by_disk,
                                           args.domain,
                                           args.raid_level,
                                           args.stripe_width,
                                           args.vpg,
                                           None,
                                           args.parity,
                                           args.node_redundancy))
        elif args.nvmesh_object == 'vpg':
            if args.name is None:
                print(formatter.yellow(
                    "VPG name missing! Use the -n argument to provide a VPG name."))
                return
            if args.size is None:
                print(formatter.yellow(
                    "Size/capacity information is missing! Use the -S argument to provide the volume size."))
                return
            if args.raid_level is None and args.vpg is None:
                print(formatter.yellow(
                    "Raid level information missing! Use the -r argument to set the raid level."))
                return
            if args.raid_level[0] == 'ec':
                if args.parity is None:
                    print(formatter.yellow(
                        "Erasure coding parity information missing! Use the -P argument to set the parity "
                        "configuration."))
                    return
                if args.node_redundancy is None:
                    print(formatter.yellow(
                        "Erasure coding node redundancy aka. protection level information missing! Use the -R argument "
                        "to set the node redundancy configuration."))
                    return
            self.poutput(manage_vpg('save',
                                    args.name,
                                    args.size,
                                    args.description,
                                    args.drive_class,
                                    args.target_class,
                                    args.domain,
                                    args.raid_level,
                                    args.stripe_width))
        cli_exit.validate_exit()

    delete_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    delete_parser.add_argument('nvmesh_object', choices=['host', 'volume', 'drive', 'driveclass', 'targetclass', 'vpg', 'nic'],
                               help='Delete hosts, servers, drives, drive classes, nic and target classes.')
    delete_parser.add_argument('-s', '--server', nargs='+',
                               help='Specify a single server or a list of servers.')
    delete_parser.add_argument('-t', '--target-class', nargs='+',
                               help='Specify a single target class or a space separated list of target classes.')
    delete_parser.add_argument('-d', '--drive-class', nargs='+',
                               help='Specify a single drive class or a space separated list of drive classes.')
    delete_parser.add_argument('-D', '--drive', nargs='+',
                               help='The drive ID of the drive to be deleted in the NVMesh cluster.')
    delete_parser.add_argument('-v', '--volume', nargs='+',
                               help='Specify a single volume or a space separated list of volumes.')
    delete_parser.add_argument('-f', '--force', required=False, action='store_const', const=True, default=False,
                               help='Use this flag to forcefully delete the volume/s.')
    delete_parser.add_argument('-N', '--nic', required=False, nargs=1,
                               help='Specify the NIC to be deleted.')
    delete_parser.add_argument('-y', '--yes', required=False, action='store_const', const=True,
                               help="Automatically answer 'yes' and skip operational warnings.")

    @with_argparser(delete_parser)
    @with_category("NVMesh Resource Management")
    def do_delete(self, args):
        """The 'delete' sub-command will let you delete nvmesh objects in your cluster or nvmesh-shell runtime
        environment. E.g. 'delete host' will delete host entries in your nvmesh-shell environment and 'delete volume'
        will delete NVMesh volumes in your NVMesh cluster."""
        action = "delete"
        if args.nvmesh_object == 'host':
            hosts.manage_hosts(action,
                               args.server,
                               False)
        elif args.nvmesh_object == 'targetclass':
            if args.target_class is None:
                print(formatter.yellow(
                    "Class information is missing! Use the -t/--target-class option to specify the class "
                    "or list of classes to be deleted."))
                return
            if args.target_class[0] == 'all':
                self.poutput(manage_target_class('delete',
                                                 get_target_class_list(),
                                                 None,
                                                 None,
                                                 None,
                                                 None))
            else:
                self.poutput(manage_target_class('delete',
                                                 args.target_class,
                                                 None,
                                                 None,
                                                 None,
                                                 None))
        elif args.nvmesh_object == 'driveclass':
            if args.drive_class is None:
                print(formatter.yellow(
                    "Class information is missing! Use the -d/--drive-class option to specify the class "
                    "or list of classes to be deleted."))
                return
            if args.drive_class[0] == 'all':
                self.poutput(manage_drive_class('delete',
                                                get_drive_class_list(),
                                                None,
                                                None,
                                                None,
                                                None,
                                                None,
                                                None))
            else:
                self.poutput(manage_drive_class('delete',
                                                args.drive_class,
                                                None,
                                                None,
                                                None,
                                                None,
                                                None,
                                                None))
        elif args.nvmesh_object == 'volume':
            if args.volume[0] == 'all':
                volume_list = get_volume_list()
            else:
                volume_list = args.volume
            if args.yes:
                self.poutput(manage_volume('remove',
                                           volume_list,
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           None,
                                           None))
            else:
                if "y" in raw_input(WARNINGS['delete_volume']).lower():
                    self.poutput(manage_volume('remove',
                                               volume_list,
                                               None,
                                               None,
                                               None,
                                               None,
                                               None,
                                               None,
                                               None,
                                               None,
                                               None,
                                               None,
                                               None,
                                               None,
                                               None))
                else:
                    return
        elif args.nvmesh_object == 'drive':
            if args.drive:
                self.poutput(manage_drive('delete', args.drive, None))
            else:
                cli_exit.error = True
                print(formatter.yellow("Use the -D/--drive argument to specify the drive to be deleted."))

        elif args.nvmesh_object == 'nic':
            if args.nic:
                self.poutput(manage_nic('delete', args.nic[0]))
            else:
                cli_exit.error = True
                print(formatter.yellow("Use the -N/--nic argument to specify the NIC to be deleted."))

        elif args.nvmesh_object == 'vpg':
            if args.name is None:
                print(formatter.yellow(
                    "VPG name missing! Use the -n argument to provide a VPG name."))
                return

            self.poutput(manage_vpg('delete',
                                    args.name[0],
                                    None,
                                    None,
                                    None,
                                    None,
                                    None,
                                    None,
                                    None))
        cli_exit.validate_exit()

    attach_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    attach_parser.add_argument('-c', '--client', nargs='+', required=True,
                               help='Specify a single server or a space separated list of servers.')
    attach_parser.add_argument('-v', '--volume', nargs='+', required=True,
                               help='Specify a single volume or a space separated list of volumes.')

    @with_argparser(attach_parser)
    @with_category("NVMesh Resource Management")
    def do_attach(self, args):
        """The 'attach' sub-command will let you attach NVMesh volumes to the clients in your NVMesh cluster."""
        if args.client[0] == 'all':
            client_list = get_client_list(True)
        else:
            client_list = args.client
        if args.volume[0] == 'all':
            volume_list = get_volume_list()
        else:
            volume_list = args.volume
        self.poutput(attach_detach_volumes('attach',
                                           client_list,
                                           volume_list))
        cli_exit.validate_exit()

    detach_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    detach_parser.add_argument('-c', '--client', nargs='+', required=True,
                               help='Specify a single server or a space separated list of servers.')
    detach_parser.add_argument('-v', '--volume', nargs='+', required=True,
                               help='Specify a single volume or a space separated list of volumes.')
    detach_parser.add_argument('-y', '--yes', required=False, action='store_const', const=True,
                               help="Automatically answer 'yes' and skip operational warnings.")

    @with_argparser(detach_parser)
    @with_category("NVMesh Resource Management")
    def do_detach(self, args):
        """The 'detach' sub-command will let you detach NVMesh volumes in your NVMesh cluster."""
        if args.client[0] == 'all':
            client_list = get_client_list(True)
        else:
            client_list = args.client
        if args.volume[0] == 'all':
            volume_list = get_volume_list()
        else:
            volume_list = args.volume
        self.poutput(attach_detach_volumes('detach',
                                           client_list,
                                           volume_list))
        cli_exit.validate_exit()

    check_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    check_parser.add_argument('nvmesh_object', choices=['client', 'target', 'manager', 'cluster'],
                              help='Specify where you want to check the NVMesh services status.')
    check_parser.add_argument('-d', '--detail', required=False, action='store_const', const=True,
                              help='Show detailed service information.')
    check_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True, default=True,
                              help='Adds the host name at the beginning of each line. This helps to identify the '
                                   'content when piping into a grep or similar')
    check_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True, default=True,
                              help='Check the hosts/servers in parallel.')
    check_parser.add_argument('-s', '--server', nargs='+', required=False,
                              help='Specify a single or a space separated list of managers, targets or clients.')

    @with_argparser(check_parser)
    @with_category("NVMesh Resource Management")
    def do_check(self, args):
        """The 'check' sub-command checks and let you list the status of the actual NVMesh services running in your
        cluster. It is using SSH connectivity to the NVMesh managers, clients or targets to verify the service status.
        E.g.'check target' will check the NVMesh target services throughout the cluster."""
        user.get_ssh_user()
        user.get_api_user()
        action = "check"
        if args.nvmesh_object == 'target':
            self.poutput(manage_nvmesh_service('target',
                                               args.detail,
                                               args.server,
                                               action,
                                               args.prefix,
                                               args.parallel,
                                               False))
        elif args.nvmesh_object == 'client':
            self.poutput(manage_nvmesh_service('client',
                                               args.detail,
                                               args.server,
                                               action,
                                               args.prefix,
                                               args.parallel,
                                               False))
        elif args.nvmesh_object == 'manager':
            self.poutput(manage_nvmesh_service('mgr',
                                               args.detail,
                                               args.server,
                                               action,
                                               args.prefix,
                                               args.parallel,
                                               False))
        elif args.nvmesh_object == 'cluster':
            manage_cluster(args.detail,
                           action,
                           args.prefix)
        cli_exit.validate_exit()

    stop_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    stop_parser.add_argument('nvmesh_object', choices=['client', 'target', 'manager', 'cluster', 'mcm'],
                             help='Specify the NVMesh service type you want to top.')
    stop_parser.add_argument('-d', '--detail', required=False, action='store_const', const=True,
                             help='List and view the service details.')
    stop_parser.add_argument('-g', '--graceful', nargs=1, required=False, default="True", choices=['True', 'False'],
                             help="Graceful stop of all NVMesh targets in the cluster."
                                  " The default is set to 'True'")
    stop_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True, default=True,
                             help='Adds the host name at the beginning of each line. This helps to identify the '
                                  'content when piping into a grep or similar')
    stop_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True, default=True,
                             help='Stop the NVMesh services in parallel.')
    stop_parser.add_argument('-s', '--server', nargs='+', required=False,
                             help='Specify a single or a space separated list of managers, targets or clients.')
    stop_parser.add_argument('-y', '--yes', required=False, action='store_const', const=True,
                             help="Automatically answer 'yes' and skip operational warnings.")

    @with_argparser(stop_parser)
    @with_category("NVMesh Resource Management")
    def do_stop(self, args):
        """The 'stop' sub-command will stop the selected NVMesh services on managers, targets or clients. Or it will
        stop the entire NVMesh cluster. It uses SSH connectivity to manage the NVMesh services. E.g. 'stop client' will
        stop all the NVMesh clients throughout the cluster."""
        user.get_ssh_user()
        user.get_api_user()
        action = "stop"
        if args.nvmesh_object == 'target':
            if args.yes:
                self.poutput(manage_nvmesh_service('target',
                                                   args.detail,
                                                   args.server,
                                                   action,
                                                   args.prefix,
                                                   args.parallel, (False if args.graceful[0] == "False" else True)))
            else:
                if "y" in raw_input(WARNINGS['stop_nvmesh_target']).lower():
                    self.poutput(manage_nvmesh_service('target',
                                                       args.detail,
                                                       args.server,
                                                       action,
                                                       args.prefix,
                                                       args.parallel, (False if args.graceful[0] == "False" else True)))
                else:
                    return
        elif args.nvmesh_object == 'client':
            if args.yes:
                self.poutput(manage_nvmesh_service('client',
                                                   args.detail,
                                                   args.server,
                                                   action,
                                                   args.prefix,
                                                   args.parallel,
                                                   False))
            else:
                if "y" in raw_input(WARNINGS['stop_nvmesh_client']).lower():
                    self.poutput(manage_nvmesh_service('client',
                                                       args.detail,
                                                       args.server,
                                                       action,
                                                       args.prefix,
                                                       args.parallel,
                                                       False))
                else:
                    return
        elif args.nvmesh_object == 'manager':
            if args.yes:
                self.poutput(manage_nvmesh_service('mgr',
                                                   args.detail,
                                                   args.server,
                                                   action,
                                                   args.prefix,
                                                   args.parallel,
                                                   False))
            else:
                if "y" in raw_input(WARNINGS['stop_nvmesh_manager']).lower():
                    self.poutput(manage_nvmesh_service('mgr',
                                                       args.detail,
                                                       args.server,
                                                       action,
                                                       args.prefix,
                                                       args.parallel,
                                                       False))
                else:
                    return
        elif args.nvmesh_object == 'cluster':
            if args.yes:
                manage_cluster(args.detail,
                               action,
                               args.prefix)
            else:
                if "y" in raw_input(WARNINGS['stop_cluster']).lower():
                    manage_cluster(args.detail,
                                   action,
                                   args.prefix)
                else:
                    return
        elif args.nvmesh_object == 'mcm':
            manage_mcm(args.server, action)
        cli_exit.validate_exit()

    start_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    start_parser.add_argument('nvmesh_object', choices=['client', 'target', 'manager', 'cluster', 'mcm'],
                              help='Specify the NVMesh service type you want to start.')
    start_parser.add_argument('-d', '--detail', required=False, action='store_const', const=True,
                              help='List and view the service details.')
    start_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True, default=True,
                              help='Adds the host name at the beginning of each line. This helps to identify the '
                                   'content when piping into a grep or similar')
    start_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True, default=True,
                              help='Start the NVMesh services on the hosts/servers in parallel.')
    start_parser.add_argument('-s', '--server', nargs='+', required=False,
                              help='Specify a single or a space separated list of servers.')

    @with_argparser(start_parser)
    @with_category("NVMesh Resource Management")
    def do_start(self, args):
        """The 'start' sub-command will start the selected NVMesh services on managers, targets or clients. Or it will
        start the entire NVMesh cluster. It uses SSH connectivity to manage the NVMesh services. E.g. 'start cluster'
        will start all the NVMesh services throughout the cluster."""
        user.get_ssh_user()
        user.get_api_user()
        action = "start"
        if args.nvmesh_object == 'target':
            self.poutput(manage_nvmesh_service('target',
                                               args.detail,
                                               args.server,
                                               action,
                                               args.prefix,
                                               args.parallel,
                                               False))
        elif args.nvmesh_object == 'client':
            self.poutput(manage_nvmesh_service('client',
                                               args.detail,
                                               args.server,
                                               action,
                                               args.prefix,
                                               args.parallel,
                                               False))
        elif args.nvmesh_object == 'manager':
            self.poutput(manage_nvmesh_service('mgr',
                                               args.detail,
                                               args.server,
                                               action,
                                               args.prefix,
                                               args.parallel,
                                               False))
        elif args.nvmesh_object == 'cluster':
            manage_cluster(args.detail,
                           action,
                           args.prefix)
        elif args.nvmesh_object == 'mcm':
            manage_mcm(args.server,
                       action)
        cli_exit.validate_exit()

    restart_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    restart_parser.add_argument('nvmesh_object', choices=['client', 'target', 'manager', 'cluster', 'mcm'],
                                help='Specify the NVMesh service which you want to restart.')
    restart_parser.add_argument('-d', '--detail', required=False, action='store_const', const=True,
                                help='List and view the service details.')
    restart_parser.add_argument('-g', '--graceful', nargs=1, required=False, default="True", choices=['True', 'False'],
                                help='Restart with a graceful stop of the targets in the cluster.'
                                     'The default is set to True')
    restart_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True, default=True,
                                help='Adds the host name at the beginning of each line. This helps to identify the '
                                     'content when piping into a grep or similar')
    restart_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True, default=True,
                                help='Restart the NVMesh services on the hosts/servers in parallel.')
    restart_parser.add_argument('-s', '--server', nargs='+', required=False,
                                help='Specify a single or a space separated list of servers.')
    restart_parser.add_argument('-y', '--yes', required=False, action='store_const', const=True,
                                help="Automatically answer 'yes' and skip operational warnings.")

    @with_argparser(restart_parser)
    @with_category("NVMesh Resource Management")
    def do_restart(self, args):
        """The 'restart' sub-command will restart the selected NVMesh services on managers, targets or clients. Or it
        will restart the entire NVMesh cluster. It uses SSH connectivity to manage the NVMesh services.
        E.g. 'restart manager' will restart the NVMesh management service."""
        user.get_ssh_user()
        user.get_api_user()
        action = 'restart'
        if args.nvmesh_object == 'target':
            if args.yes:
                self.poutput(manage_nvmesh_service('target',
                                                   args.detail,
                                                   args.server,
                                                   action,
                                                   args.prefix,
                                                   args.parallel, (False if args.graceful[0] == "False" else True)))
            else:
                if "y" in raw_input(WARNINGS['stop_nvmesh_target']).lower():
                    self.poutput(manage_nvmesh_service('target',
                                                       args.detail,
                                                       args.server,
                                                       action,
                                                       args.prefix,
                                                       args.parallel, (False if args.graceful[0] == "False" else True)))
                else:
                    return
        elif args.nvmesh_object == 'client':
            if args.yes:
                self.poutput(manage_nvmesh_service('client',
                                                   args.detail,
                                                   args.server,
                                                   action,
                                                   args.prefix,
                                                   args.parallel,
                                                   False))
            else:
                if "y" in raw_input(WARNINGS['stop_nvmesh_client']).lower():
                    self.poutput(manage_nvmesh_service('client',
                                                       args.detail,
                                                       args.server,
                                                       action,
                                                       args.prefix,
                                                       args.parallel,
                                                       False))
                else:
                    return
        elif args.nvmesh_object == 'manager':
            if args.yes:
                self.poutput(manage_nvmesh_service('mgr',
                                                   args.detail,
                                                   args.server,
                                                   action,
                                                   args.prefix,
                                                   args.parallel,
                                                   False))
            else:
                if "y" in raw_input(WARNINGS['stop_nvmesh_manager']).lower():
                    self.poutput(manage_nvmesh_service('mgr',
                                                       args.detail,
                                                       args.server,
                                                       action,
                                                       args.prefix,
                                                       args.parallel,
                                                       False))
                else:
                    return
        elif args.nvmesh_object == 'mcm':
            manage_mcm(args.server,
                       action)
        elif args.nvmesh_object == 'cluster':
            manage_cluster(args.detail,
                           action,
                           args.prefix)
        cli_exit.validate_exit()

    define_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    define_parser.add_argument('nvmesh_object', choices=['manager', 'sshuser', 'apiuser'],
                               help='Specify the NVMesh shell runtime variable you want to define.')

    @with_argparser(define_parser)
    @with_category("NVMesh Resource Management")
    def do_define(self, args):
        """The 'define' sub-command defines/sets the shell runtime variables as NVMesh management servers and user
        credentials to be used. E.g. 'define apiuser' will set the NVMesh API user name to be used for all the
        operations involving the API"""
        if args.nvmesh_object == 'sshuser':
            user.SSH_user_name = raw_input("Please provide the user name to be used for SSH connectivity: ")
            user.SSH_password = getpass.getpass("Please provide the SSH password: ")
            if 'y' in raw_input("Do you require sudo for SSH remote command execution? [Yes|No] :"):
                user.save_ssh_sudo(True)
            else:
                user.save_ssh_sudo(False)
            user.save_ssh_user()
        elif args.nvmesh_object == 'apiuser':
            user.API_user_name = raw_input("Please provide a administrative API user name: ")
            user.API_password = getpass.getpass("Please provide the API password: ")
            user.save_api_user()
        elif args.nvmesh_object == 'manager':
            ManagementServer().save_management_server(raw_input(
                "Provide a space separated list, min. one, of the NVMesh manager server name/s: ").split(" "))
        cli_exit.validate_exit()

    runcmd_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    runcmd_parser.add_argument('scope', choices=['client', 'target', 'manager', 'cluster', 'host'],
                               help='Specify the scope where you want to run the command.')
    runcmd_parser.add_argument('-c', '--command', nargs='+', required=True,
                               help='The command you want to run on the servers. Use quotes if the command needs to run'
                                    ' with flags by itself, like: runcmd cluster -c "uname -a"')
    runcmd_parser.add_argument('-p', '--prefix', required=False, action='store_const', const=True, default=True,
                               help='Adds the host name at the beginning of each line. This helps to identify the '
                                    'content when piping into a grep or similar tasks.')
    runcmd_parser.add_argument('-P', '--parallel', required=False, action='store_const', const=True, default=True,
                               help='Runs the remote command on the remote hosts in parallel.')
    runcmd_parser.add_argument('-s', '--server', nargs='+', required=False,
                               help='Specify list of servers and or hosts.')

    @with_argparser(runcmd_parser)
    @with_category("NVMesh Resource Management")
    def do_runcmd(self, args):
        """Run a remote shell command across the whole NVMesh cluster, or just the targets, clients, managers or a list
        of selected servers and hosts. Excample: runcmd manager -c systemctl status mongod"""
        user.get_ssh_user()
        user.get_api_user()
        self.poutput(self.run_command(args.command,
                                      args.scope,
                                      args.prefix,
                                      args.parallel,
                                      args.server))
        cli_exit.validate_exit()

    @staticmethod
    def run_command(command, scope, prefix, parallel, server_list):
        try:
            host_list = []
            ssh = SSHRemoteOperations()
            command_line = " ".join(command)
            if server_list is not None:
                host_list = server_list
            else:
                if scope == 'cluster':
                    host_list = get_target_list(short=True)
                    host_list.extend(get_client_list(False))
                    host_list.extend(mgmt.get_management_server_list())
                if scope == 'target':
                    host_list = get_target_list(short=True)
                if scope == 'client':
                    host_list = get_client_list(False)
                if scope == 'manager':
                    host_list = mgmt.get_management_server_list()
                if scope == 'host':
                    host_list = Hosts().manage_hosts('get', None, False)
            host_list = set(host_list)
            command_return_list = []
            if parallel is True:
                process_pool = Pool(len(host_list))
                parallel_execution_map = []
                for host in host_list:
                    parallel_execution_map.append([host, command_line])
                command_return_list = process_pool.map(run_parallel_ssh_command, parallel_execution_map)
                process_pool.close()
            else:
                for host in host_list:
                    command_return = ssh.return_remote_command_std_output(host, command_line)
                    if command_return:
                        command_return_list.append([host, command_return])
            output = []
            for command_return in command_return_list:
                if command_return[1][0] != 0:
                    cli_exit.error = True
                    output_line = formatter.red(" ".join(["Return Code %s," % (
                        command_return[1][0]), command_return[1][1]]))
                    if prefix is True:
                        output.append(formatter.add_line_prefix(command_return[0], output_line, True))
                    else:
                        output.append(output_line)
                else:
                    if len(command_return[1][1]) < 1:
                        output_line = formatter.green("OK")
                    else:
                        output_line = command_return[1][1]
                    if prefix is True:
                        output.append(formatter.add_line_prefix(command_return[0], output_line, True))
                    else:
                        output.append(output_line)
            return "\n".join(output)
        except Exception, e:
            print(formatter.red("Error: " + e.message))
            logging.critical(e.message)
            cli_exit.error = True

    testssh_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    testssh_parser.add_argument('-s', '--server', nargs='+', required=False,
                                help='Specify a server or a list of servers and/or hosts.')

    @with_argparser(testssh_parser)
    @with_category("NVMesh Resource Management")
    def do_testssh(self, args):
        """Test the SSH connectivity to all, a list of, or individual servers and hosts.
        Excample: testssh -s servername"""
        ssh = SSHRemoteOperations()
        user.get_ssh_user()
        ssh.test_ssh_connection(args.server)
        cli_exit.validate_exit()

    update_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    update_parser.add_argument('object', choices=['volume', 'driveclass', 'targetclass'],
                               help='Specify the NVMesh object to be updated.')
    update_parser.add_argument('-n', '--name', nargs=1, required=True,
                               help='The name of the object to be updated.')
    update_parser.add_argument('-S', '--size', nargs='+', required=False,
                               help='The new/updated size/capacity of the volume.\n '
                                    'The volumes size value is base*2/binary. \n'
                                    'Example: -s 12GB or 12GiB will size the volume with a size of 12884901888 bytes.\n'
                                    'Some valid input formats samples: xGB, x GB, x gigabyte, x GiB or xG')
    update_parser.add_argument('-D', '--description', required=False, nargs='+',
                               help='The new/updated name of the NVMesh object.')
    update_parser.add_argument('-s', '--server', nargs='+', required=False, help='Specify a single server or a space '
                                                                                 'separated list of servers.')
    drive_file_group = update_parser.add_mutually_exclusive_group()
    drive_file_group.add_argument('-m', '--drive', nargs='+', required=False,
                                  help='Drive/media information. Needs to include the drive ID/serial and the target'
                                       'node/server name in the format driveId:targetName'
                                       'Example: -m "Example: 174019659DA4.1:test.lab"')
    drive_file_group.add_argument('-f', '--file', nargs=1, required=False,
                                  help='Path to the file containing the driveId:targetName information. '
                                       'Needs to'
                                       'Example: -f "/path/to/file". This argument is not allowed together with the -m '
                                       'argument')
    update_parser.add_argument('-l', '--limit-by-disk', nargs='+', required=False,
                               help='Optional - Limit volume allocation to specific drives.')
    update_parser.add_argument('-L', '--limit-by-target', nargs='+', required=False,
                               help='Optional - Limit volume allocation to specific target nodes.')
    update_parser.add_argument('-t', '--target-class', nargs='+', required=False,
                               help='Optional - Limit volume allocation to specific target classes.')
    update_parser.add_argument('-d', '--drive-class', nargs='+', required=False,
                               help='Optional - Limit volume allocation to specific drive classes.')

    @with_argparser(update_parser)
    @with_category("NVMesh Resource Management")
    def do_update(self, args):
        """Update and edit an existing NVMesh volume, driveclass or targetclass."""
        if get_api_ready() == 0:
            if args.object == 'volume':
                volume = json.loads(nvmesh.get_volume(args.name[0]))
                if len(volume) == 0:
                    print(formatter.yellow("%s is not a valid volume name. A volume with this name doesn't exist."
                                           % args.name[0]))
                    return
                else:
                    self.poutput(update_volume(volume[0],
                                               args.size,
                                               args.description,
                                               args.limit_by_disk,
                                               args.limit_by_target,
                                               args.drive_class,
                                               args.target_class))
            elif args.object == 'targetclass':
                target_class = json.loads(nvmesh.get_target_class(args.name[0]))
                if len(target_class) == 0:
                    print(formatter.yellow("%s is not a valid target class name. "
                                           "A target class with this name doesn't exist."
                                           % args.name[0]))
                    return
                else:
                    self.poutput(update_target_class(target_class[0],
                                                     args.server,
                                                     args.description))
            elif args.object == 'driveclass':
                drive_class = json.loads(nvmesh.get_drive_class(args.name[0]))
                if len(drive_class) == 0:
                    print(formatter.yellow(
                        "%s is not a valid drive class name. A drive class with this name doesn't exist."
                        % args.name[0]))
                    return
                else:
                    self.poutput(update_drive_class(drive_class[0],
                                                    args.drive,
                                                    args.description,
                                                    args.file))
        cli_exit.validate_exit()

    evict_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    evict_parser.add_argument('-d', '--drive', nargs='+', required=True,
                              help="The drive ID of the drive to evict.")
    evict_parser.add_argument('-y', '--yes', required=False, action='store_const', const=True,
                              help="Automatically answer 'yes' and skip operational warnings.")

    @with_argparser(evict_parser)
    @with_category("NVMesh Resource Management")
    def do_evict(self, args):
        """Evict a drive in the NVMesh cluster."""
        try:
            if args.yes:
                self.poutput(manage_drive('evict', args.drive, None))
            else:
                if 'y' in raw_input(WARNINGS['evict_drive']):
                    self.poutput(manage_drive('evict', args.drive, None))
        except Exception, e:
            print(formatter.red("Error: " + e.message))
            logging.critical(e.message)
            cli_exit.error = True

    format_parser = argparse.ArgumentParser(formatter_class=ArgsUsageOutputFormatter)
    format_parser.add_argument('-d', '--drive', nargs='+', required=True,
                               help="The drive ID or space separated list of drive IDs to be formatted.")
    format_parser.add_argument('-f', '--format', nargs=1, required=True,
                               help="The format to be used. Valid options are: 'legacy' for NVMesh RAID-0, 1, 10, and "
                                    "Concatenated volumes, and 'ec' to support the new NVMesh distributed EC parity "
                                    "feature.")
    format_parser.add_argument('-y', '--yes', required=False, action='store_const', const=True,
                               help="Automatically answer 'yes' and skip operational warnings.")

    @with_argparser(format_parser)
    @with_category("NVMesh Resource Management")
    def do_format(self, args):
        """Format a drive in the NVMesh cluster."""
        try:
            if args.yes:
                self.poutput(manage_drive('format', args.drive, args.format[0]))
            else:
                if 'y' in raw_input(WARNINGS['format_drive']):
                    self.poutput(manage_drive('format', args.drive, args.format[0]))
        except Exception, e:
            print(formatter.red("Error: " + e.message))
            logging.critical(e.message)
            cli_exit.error = True

    def do_exit(self, _):
        exit()


def get_api_ready():
    user.get_api_user()
    nvmesh.user_name = user.API_user_name
    nvmesh.password = user.API_password
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    manager_list = mgmt.get_management_server_list()
    for manager in manager_list:
        nvmesh.server = manager.strip()
        try:
            nvmesh.login()
            return 0
        except Exception, e:
            if len(manager_list) < 2:
                message = "\n".join(["Cannot log into management server %s!" % manager.strip(),
                                     "Currently defined servers in the cli tool:",
                                     "\n".join(sorted(open(ManagementServer().server_file).read().splitlines()))])
                logging.critical("\n".join([message, str(e.message)]))
                print(formatter.red(message))
                cli_exit.error = True
                cli_exit.validate_exit()
                return 1
            else:
                if manager_list.index(manager) + 1 == len(manager_list):
                    message = "\n".join(["Cannot log into any management server as defined in the nvmesh cli list! "
                                         "Use 'define manager' to update and correct the list of management servers to "
                                         "be used by the cli tool.", "Currently defined servers in the cli tool:",
                                         "\n".join(sorted(open(ManagementServer().server_file).read().splitlines()))])
                    logging.critical("\n".join([message, str(e.message)]))
                    print(formatter.red(message))
                    cli_exit.error = True
                    cli_exit.validate_exit()
                    return 1
                message = "Cannot log into management server %s, Trying the next one in the list." % manager.strip()
                print(formatter.yellow(message))
                logging.warning("\n".join([message, str(e.message)]))
                continue


def show_cluster(csv_format, json_format):
    try:
        if get_api_ready() == 0:
            cluster_json = json.loads(nvmesh.get_cluster())
            capacity_json = json.loads(nvmesh.get_space_allocation())
            total_server = cluster_json['servers']['totalServers']
            offline_server = cluster_json['servers']['offlineServers']
            total_clients = cluster_json['clients']['totalClients']
            offline_clients = cluster_json['clients']['offlineClients']
            cluster_volumes = []
            cluster_list = []
            for volume, count in cluster_json['volumes'].items():
                cluster_volumes.append(' '.join([repr(count), volume]))
            cluster_list.append([total_server, offline_server, total_clients, offline_clients,
                                 '; '.join(cluster_volumes),
                                 humanfriendly.format_size(capacity_json['totalCapacityInBytes'], binary=True),
                                 humanfriendly.format_size(capacity_json['availableSpaceInBytes'], binary=True)])
            if csv_format is True:
                return formatter.print_tsv(cluster_list)
            elif json_format is True:
                return formatter.print_json(cluster_list)
            else:
                return format_smart_table(cluster_list,
                                          ['Total Servers',
                                           'Offline Servers',
                                           'Total Clients',
                                           'Offline Clients',
                                           'Volumes',
                                           'Total Capacity',
                                           'Available Space'])
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def show_target(details, csv_format, json_format, server, short):
    try:
        if get_api_ready() == 0:
            target_json = json.loads(nvmesh.get_servers())
            target_list = []
            for target in target_json:
                if server is not None and target['node_id'].split('.')[0] not in server:
                    continue
                else:
                    if short is True:
                        target_name = target['node_id'].split('.')[0]
                    else:
                        target_name = target['node_id']
                    target_disk_list = []
                    target_nic_list = []
                    if target["health"] == "healthy":
                        health = formatter.green(formatter.bold("Healthy ")) + u'\u2705'
                    else:
                        health = formatter.red(formatter.bold("Critical ")) + u'\u274C'
                    for disk in target['disks']:
                        target_disk_list.append(disk['diskID'])
                    for nic in target['nics']:
                        target_nic_list.append(nic['nicID'])
                    if details is True:
                        target_list.append([target_name,
                                            health, target['version'],
                                            ' '.join(target_disk_list),
                                            ' '.join(target_nic_list)])
                    else:
                        target_list.append([target_name, health, target['version']])
            if details is True:
                if csv_format is True:
                    return formatter.print_tsv(target_list)
                elif json_format is True:
                    formatter.print_json(target_list)
                    return
                else:
                    return format_smart_table(sorted(target_list),
                                              ['Target Name',
                                               'Target Health',
                                               'NVMesh Version',
                                               'Target Disks',
                                               'Target NICs'])
            else:
                if csv_format is True:
                    return formatter.print_tsv(target_list)
                elif json_format is True:
                    return formatter.print_json(target_list)
                else:
                    return format_smart_table(sorted(target_list), ['Target Name',
                                                                    'Target Health',
                                                                    'NVMesh Version'])
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def get_target_list(short):
    try:
        if get_api_ready() == 0:
            target_json = json.loads(nvmesh.get_servers())
            target_list = []
            for target in target_json:
                if short:
                    target_list.append(target['node_id'].split('.')[0])
                else:
                    target_list.append(target['node_id'])
            return target_list
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def get_client_list(full):
    try:
        if get_api_ready() == 0:
            clients_json = json.loads(nvmesh.get_clients())
            client_list = []
            for client in clients_json:
                if full is True:
                    client_list.append(client['client_id'])
                else:
                    client_list.append(client['client_id'].split('.')[0])
            return client_list
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def get_volume_list():
    try:
        if get_api_ready() == 0:
            volume_json = json.loads(nvmesh.get_volumes())
            volume_list = []
            for volume in volume_json:
                volume_list.append(volume['_id'].split('.')[0])
            return volume_list
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def get_drive_class_list():
    try:
        if get_api_ready() == 0:
            drive_class_list = []
            drive_class_json = json.loads(nvmesh.get_disk_classes())
            for drive_class in drive_class_json:
                drive_class_list.append(drive_class["_id"])
            return drive_class_list
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def get_target_class_list():
    try:
        if get_api_ready() == 0:
            target_class_list = []
            target_class_json = json.loads(nvmesh.get_target_classes())
            for target_class in target_class_json:
                target_class_list.append(target_class["_id"])
            return target_class_list
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def show_manager():
    try:
        if get_api_ready() == 0:
            manager_list = []
            manager_json = json.loads(nvmesh.get_managers())
            for manager in manager_json:
                manager_list.append([
                    manager["hostname"],
                    manager["ip"],
                    u'\N{check mark}' if "isMe" in manager else " ",
                    u'\N{check mark}' if manager["useSSL"] else " ",
                    manager["port"],
                    manager["outbound_socket_status"] if "isMe" not in manager else "n/a",
                    manager["inbound_socket_status"] if "isMe" not in manager else "n/a"
                ])
            return format_smart_table(sorted(manager_list), [
                "Manager",
                "IP",
                "Current Connection",
                "Use SSL",
                "Port",
                "Outbound Socket",
                "Inbound Socket"])
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def get_manager_list(short):
    try:
        if get_api_ready() == 0:
            manager_list = []
            manager_json = json.loads(nvmesh.get_managers())
            for manager in manager_json:
                if short:
                    manager_list.append(manager["hostname"].split(".")[0])
                else:
                    manager_list.append(manager["hostname"])
            return sorted(manager_list) if manager_list is not None else None
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def show_clients(csv_format, json_format, server, short):
    try:
        if get_api_ready() == 0:
            clients_json = json.loads(nvmesh.get_clients())
            client_list = []
            for client in clients_json:
                if server is not None and client['client_id'].split('.')[0] not in server:
                    continue
                else:
                    volume_list = []
                    if client["health"] == "healthy":
                        health = formatter.green(formatter.bold("Healthy ")) + u'\u2705'
                    else:
                        health = formatter.red(formatter.bold("Critical ")) + u'\u274C'
                    if short is True:
                        client_name = client['client_id'].split('.')[0]
                    else:
                        client_name = client['client_id']
                    for volume in client['block_devices']:
                        if volume['vol_status'] == 4:
                            volume_list.append(volume['name'])
                    client_list.append(
                        [client_name, health, client['version'], ' '.join(sorted(set(volume_list)))])
            if csv_format is True:
                return formatter.print_tsv(client_list)
            elif json_format is True:
                return formatter.print_json(client_list)
            else:
                return format_smart_table(sorted(client_list),
                                          ['Client Name',
                                           'Client Health',
                                           'Client Version',
                                           'Client Volumes'])
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def show_volumes(details, csv_format, json_format, volumes, short, layout):
    try:
        if get_api_ready() == 0:
            volumes_json = json.loads(nvmesh.get_volumes())
            volumes_list = []
            for volume in volumes_json:
                remaining_dirty_bits = 0
                name = formatter.bold(volume["name"])
                if volume["health"] == "healthy":
                    health = formatter.green(formatter.bold("Healthy"))
                    status = formatter.green(formatter.bold(volume["status"].capitalize()))
                elif volume["health"] == "alarm":
                    health = formatter.yellow(formatter.bold("Alarm"))
                    status = formatter.yellow(formatter.bold(volume["status"].capitalize()))
                else:
                    health = formatter.red(formatter.bold("Critical"))
                    status = formatter.red(formatter.bold(volume["status"].capitalize()))

                if volumes is not None and volume['name'] not in volumes:
                    continue
                else:
                    if 'stripeWidth' in volume:
                        stripe_width = volume['stripeWidth']
                    else:
                        stripe_width = None
                    if 'domain' in volume:
                        awareness_domain = volume['domain']
                    else:
                        awareness_domain = None
                    if 'serverClasses' in volume:
                        if len(volume['serverClasses']) > 0:
                            target_classes_list = volume['serverClasses']
                        else:
                            target_classes_list = None
                    else:
                        target_classes_list = None

                    if 'diskClasses' in volume:
                        if len(volume['diskClasses']) > 0:
                            drive_classes_list = volume['diskClasses']
                        else:
                            drive_classes_list = None
                    else:
                        drive_classes_list = None
                    if 'dataBlocks' in volume:
                        data_blocks = str(volume['dataBlocks'])
                    if 'parityBlocks' in volume:
                        parity_blocks = str(volume['parityBlocks'])
                    if volume['RAIDLevel'].lower() == "erasure coding":
                        parity_info = "+".join([data_blocks, parity_blocks])
                        protection_level = volume['protectionLevel']
                        stripe_width = "n/a"
                    else:
                        parity_info = "n/a"
                        protection_level = "n/a"

                    target_list = []
                    target_disk_list = []
                    chunk_count = 0
                    volume_layout_list = []

                    if layout:
                        for chunk in volume['chunks']:
                            for praid in chunk['pRaids']:
                                for segment in praid['diskSegments']:
                                    volume_layout_list.append([str(chunk_count),
                                                               str(praid['stripeIndex']),
                                                               str(segment['pRaidIndex']),
                                                               segment['type'],
                                                               str(segment['lbs']) if segment['lbs'] != 0 else "n/a",
                                                               str(segment['lbe']) if segment['lbe'] != 0 else "n/a",
                                                               u'\u274C' if segment['isDead'] is True else u'\u2705',
                                                               segment['diskID'],
                                                               segment['node_id']])
                            chunk_count += 1

                    for chunk in volume['chunks']:
                        for praid in chunk['pRaids']:
                            for segment in praid['diskSegments']:
                                if segment['type'] == 'raftonly':
                                    continue
                                else:
                                    if "remainingDirtyBits" in segment:
                                        remaining_dirty_bits = remaining_dirty_bits + segment['remainingDirtyBits']
                                    target_disk_list.append(segment['diskID'])
                                    if short is True:
                                        target_list.append(segment['node_id'].split('.')[0])
                                    else:
                                        target_list.append(segment['node_id'])

                    if details is True and not layout:
                        volumes_list.append([name,
                                             health,
                                             status if remaining_dirty_bits == 0 else " ".join([
                                                 status, str(
                                                     100 - ((remaining_dirty_bits * 4096) * 100 / (int(volume['blocks'])
                                                     * int(volume['blockSize'])))) + "%"]),
                                             volume['RAIDLevel'],
                                             parity_info,
                                             protection_level,
                                             humanfriendly.format_size((int(volume['blocks'])
                                                                        * int(volume['blockSize'])), binary=True),
                                             stripe_width if stripe_width is not None else "n/a",
                                             humanfriendly.format_size((remaining_dirty_bits * 4096), binary=True),
                                             ' '.join(set(target_list)),
                                             ' '.join(set(target_disk_list)),
                                             ' '.join(target_classes_list) if target_classes_list is not None
                                             else "n/a",
                                             ' '.join(drive_classes_list) if drive_classes_list is not None else "n/a",
                                             awareness_domain if awareness_domain is not None else "n/a"])

                    elif details is True and layout:
                        volumes_list.append([name,
                                             health,
                                             status if remaining_dirty_bits == 0 else " ".join([
                                                 status, str(
                                                     100 - ((remaining_dirty_bits * 4096) * 100 / (int(volume['blocks'])
                                                                                                   * int(
                                                                 volume['blockSize'])))) + "%"]),
                                             volume['RAIDLevel'],
                                             parity_info,
                                             protection_level,
                                             humanfriendly.format_size((int(volume['blocks'])
                                                                        * int(volume['blockSize'])), binary=True),
                                             stripe_width if stripe_width is not None else "n/a",
                                             humanfriendly.format_size((remaining_dirty_bits * 4096), binary=True),
                                             ' '.join(set(target_list)),
                                             ' '.join(set(target_disk_list)),
                                             ' '.join(target_classes_list) if target_classes_list is not None
                                             else "n/a",
                                             ' '.join(drive_classes_list) if drive_classes_list is not None else "n/a",
                                             awareness_domain if awareness_domain is not None else "n/a",
                                             format_smart_table(volume_layout_list, ["Chunk",
                                                                                     "Stripe",
                                                                                     "Segment",
                                                                                     "Type",
                                                                                     "LBA Start",
                                                                                     "LBA End",
                                                                                     "Status",
                                                                                     "Disk ID",
                                                                                     "Last Known Target"])])
                    else:
                        volumes_list.append([name,
                                             health,
                                             status if remaining_dirty_bits == 0 else " ".join([
                                                 status, str(
                                                     100 - ((remaining_dirty_bits * 4096) * 100 / (int(volume['blocks'])
                                                                                                   * int(
                                                                 volume['blockSize'])))) + "%"]),
                                             volume['RAIDLevel'],
                                             parity_info,
                                             protection_level,
                                             humanfriendly.format_size((int(volume['blocks'])
                                                                        * int(volume['blockSize'])), binary=True),
                                             stripe_width if stripe_width is not None else "n/a",
                                             humanfriendly.format_size((remaining_dirty_bits * 4096), binary=True)])
            if details is True and not layout:
                if csv_format is True:
                    return formatter.print_tsv(volumes_list)
                elif json_format is True:
                    return formatter.print_json(volumes_list)
                else:
                    return format_smart_table(sorted(volumes_list),
                                              ['Volume Name',
                                               'Volume Health',
                                               'Volume Status',
                                               'Volume Type',
                                               'Parity Info',
                                               'Protection Level',
                                               'Volume Size',
                                               'Stripe Width',
                                               'Dirty Bits',
                                               'Target Names',
                                               'Target Disks',
                                               'Target Classes',
                                               'Drive Classes',
                                               'Awareness/Domain'])
            elif details is True and layout:
                if csv_format is True:
                    return formatter.print_tsv(volumes_list)
                elif json_format is True:
                    return formatter.print_json(volumes_list)
                else:
                    return format_smart_table(sorted(volumes_list),
                                              ['Volume Name',
                                               'Volume Health',
                                               'Volume Status',
                                               'Volume Type',
                                               'Parity Info',
                                               'Protection Level',
                                               'Volume Size',
                                               'Stripe Width',
                                               'Dirty Bits',
                                               'Target Names',
                                               'Target Disks',
                                               'Target Classes',
                                               'Drive Classes',
                                               'Awareness/Domain',
                                               'Volume Layout'])
            else:
                if csv_format is True:
                    return formatter.print_tsv(volumes_list)
                elif json_format is True:
                    return formatter.print_json(volumes_list)
                else:
                    return format_smart_table(sorted(volumes_list),
                                              ['Volume Name',
                                               'Volume Health',
                                               'Volume Status',
                                               'Volume Type',
                                               'Parity Info',
                                               'Protection Level',
                                               'Volume Size',
                                               'Stripe Width',
                                               'Dirty Bits'])
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def show_vpgs(csv_format, json_format, vpgs):
    try:
        if get_api_ready() == 0:
            vpgs_json = json.loads(nvmesh.get_vpgs())
            vpgs_list = []
            for vpg in vpgs_json:
                server_classes_list = []
                disk_classes_list = []
                if vpgs is not None and vpg['name'] not in vpgs:
                    continue
                else:
                    vpg_description = vpg['description'] if 'description' in vpg else " "
                    if 'stripeWidth' not in vpg:
                        vpg_stripe_width = ''
                    else:
                        vpg_stripe_width = vpg['stripeWidth']
                    for disk_class in vpg['diskClasses']:
                        disk_classes_list.append(disk_class)
                    for server_class in vpg['serverClasses']:
                        server_classes_list.append(server_class)

                vpgs_list.append(
                    [vpg['name'],
                     vpg_description,
                     vpg['RAIDLevel'],
                     vpg_stripe_width,
                     humanfriendly.format_size(vpg['capacity'], binary=True),
                     '; '.join(disk_classes_list),
                     '; '.join(server_classes_list)])
            if csv_format is True:
                return formatter.print_tsv(vpgs_list)
            elif json_format is True:
                return formatter.print_json(vpgs_list)
            else:
                return format_smart_table(vpgs_list, ['VPG Name',
                                                      'Description',
                                                      'RAID Level',
                                                      'Stripe Width',
                                                      'Reserved Capacity',
                                                      'Disk Classes',
                                                      'Target Classes'])
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def show_drive_classes(details, csv_format, json_format, classes):
    try:
        if get_api_ready() == 0:
            drive_classes_json = json.loads(nvmesh.get_disk_classes())
            drive_class_list = []
            for drive_class in drive_classes_json:
                drive_model_list = []
                drive_target_list = []
                domain_list = []
                if classes is not None and drive_class['_id'] not in classes:
                    continue
                else:
                    if 'domains' in drive_class:
                        for domain in drive_class['domains']:
                            domain_list.append("scope:" + domain['scope'] + " identifier:" + domain['identifier'])
                    else:
                        domain_list = None
                    for disk in drive_class['disks']:
                        drive_model_list.append(disk['model'])
                        if disk["disks"]:
                            for drive in disk['disks']:
                                if details is True:
                                    drive_target_list.append(' '.join([drive['diskID'],
                                                                       drive['node_id']]))
                                else:
                                    drive_target_list.append(drive['diskID'])
                        else:
                            drive_target_list = []
                    drive_class_list.append([drive_class['_id'],
                                             '; '.join(drive_model_list),
                                             '; '.join(drive_target_list),
                                             '; '.join(domain_list) if domain_list is not None else "n/a"])
            if csv_format is True:
                return formatter.print_tsv(drive_class_list)
            elif json_format is True:
                return formatter.print_json(drive_class_list)
            else:
                return format_smart_table(drive_class_list, ['Drive Class',
                                                             'Drive Model',
                                                             'Drive Details',
                                                             'Awareness/Domains'])
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def show_logs(all_logs):
    try:
        if get_api_ready() == 0:
            logs_list = []
            logs_json = json.loads(nvmesh.get_logs(all_logs))
            for log_entry in logs_json:
                if log_entry["level"] == "ERROR":
                    logs_list.append(
                        "\t".join([str(dateutil.parser.parse(log_entry["timestamp"])),
                                   formatter.red(log_entry["level"]),
                                   log_entry["message"]]))
                elif log_entry["level"] == "WARNING":
                    logs_list.append(
                        "\t".join([str(dateutil.parser.parse(log_entry["timestamp"])),
                                   formatter.yellow(log_entry["level"]),
                                   log_entry["message"]]).strip())
                else:
                    logs_list.append(
                        "\t".join(
                            [str(dateutil.parser.parse(log_entry["timestamp"])),
                             log_entry["level"],
                             log_entry["message"]]).strip())
            return "\n".join(logs_list)
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def show_target_classes(csv_format, json_format, classes):
    try:
        if get_api_ready() == 0:
            target_classes_json = json.loads(nvmesh.get_target_classes())
            target_classes_list = []
            for target_class in target_classes_json:
                if classes is not None and target_class['_id'] not in classes:
                    continue
                else:
                    target_nodes = []
                    domain_list = []
                    if 'domains' in target_class:
                        for domain in target_class['domains']:
                            domain_list.append("scope:" + domain['scope'] + " identifier:" + domain['identifier'])
                    else:
                        domain_list = None
                    if 'description' not in target_class:
                        target_class_description = "n/a"
                    else:
                        target_class_description = target_class['description']
                    for node in target_class['targetNodes']:
                        target_nodes.append(node)

                target_classes_list.append([target_class['name'],
                                            target_class_description,
                                            '; '.join(target_nodes),
                                            '; '.join(domain_list) if domain_list is not None else "n/a"])
            if csv_format is True:
                return formatter.print_tsv(target_classes_list)
            elif json_format is True:
                return formatter.print_json(target_classes_list)
            else:
                return format_smart_table(target_classes_list, ['Target Class',
                                                                'Description',
                                                                'Target Nodes',
                                                                'Awareness/Domains'])
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def count_active_targets():
    try:
        active_targets = 0
        for target in get_target_list(short=True):
            ssh = SSHRemoteOperations()
            ssh_return = ssh.return_remote_command_std_output(target, "/etc/init.d/nvmeshtarget status")
            if ssh_return[0] == 0:
                active_targets += 1
        return active_targets
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def parse_domain_args(args_list):
    if args_list is None:
        return None
    else:
        domain_list = []
        domain_dict = {}
        for line in args_list:
            domain_dict["scope"] = line.split("&")[0].split(":")[1]
            domain_dict["identifier"] = line.split("&")[1].split(":")[1]
            domain_list.append(domain_dict)
        return domain_list


def parse_drive_args(args_drive_list):
    try:
        if args_drive_list is None:
            return None
        drive_list = []
        for drive in args_drive_list:
            drive_list.append(
                {
                    "diskID": drive.split(":")[0],
                    "node_id": drive.split(":")[1].strip()
                }
            )
        return drive_list
    except Exception, e:
        cli_exit.error = True
        logging.critical(e.message)
        print(formatter.red("Error: " + e.message))


def manage_nvmesh_service(scope, details, servers, action, prefix, parallel, graceful):
    output = []
    ssh = SSHRemoteOperations()
    host_list = []
    ssh_return = []

    if servers is not None:
        host_list = set(servers)

    else:
        if scope == 'cluster':
            host_list = get_target_list(short=True)
            host_list.extend(get_client_list(False))
            host_list.extend(mgmt.get_management_server_list())
        if scope == 'target':
            host_list = get_target_list(short=True)
        if scope == 'client':
            host_list = get_client_list(False)
        if scope == 'mgr':
            if action is not "start":
                host_list = get_manager_list(short=True)
            else:
                host_list = ManagementServer().get_management_server_list()

    if host_list is None:
        error_message = "Cannot get server/client list and/or verify NVMesh manager from NVMesh management!"
        logging.critical(error_message)
        print(formatter.red(error_message))
        cli_exit.error = True
        return

    if scope == "target":
        if action == "stop" and servers is None and graceful:
            nvmesh.target_cluster_shutdown({"control": "shutdownAll"})
            print("\n".join(["Shutting down the NVMesh target services in the cluster.", "Please wait..."]))
            while count_active_targets() != 0:
                time.sleep(5)
            print(" ".join(["All target services shut down.", formatter.green("OK")]))
            return

    if parallel:
        if host_list and len(host_list) > 0:
            process_pool = Pool(len(set(host_list)))
        else:
            return
        parallel_execution_map = []
        for host in set(host_list):
            if action == "check":
                parallel_execution_map.append([host, "/opt/NVMesh/%s*/services/nvmesh%s status" % (scope[0], scope)])
            elif action == "start":
                parallel_execution_map.append([host, "/opt/NVMesh/%s*/services/nvmesh%s start" % (scope[0], scope)])
            elif action == "stop":
                parallel_execution_map.append([host, "/opt/NVMesh/%s*/services/nvmesh%s stop" % (scope[0], scope)])
            elif action == "restart":
                parallel_execution_map.append([host, "/opt/NVMesh/%s*/services/nvmesh%s restart" % (scope[0], scope)])

        command_return_list = process_pool.map(run_parallel_ssh_command, parallel_execution_map)
        process_pool.close()
        for command_return in command_return_list:
            try:
                if command_return[1][0] == 0:
                    if details is True:
                        output.append(formatter.bold(" ".join([command_return[0],
                                                               action.capitalize(),
                                                               formatter.green('OK')])))
                        if prefix is True:
                            output.append(formatter.add_line_prefix(command_return[0], (
                                command_return[1][1][:command_return[1][1].rfind('\n')]), True) + "\n")
                        else:
                            output.append((command_return[1][1][:command_return[1][1].rfind('\n')] + "\n"))
                    else:
                        output.append(" ".join([command_return[0],
                                                action.capitalize(),
                                                formatter.green('OK')]))
                else:
                    cli_exit.error = True
                    if details is True:
                        output.append(formatter.bold(" ".join([command_return[0],
                                                               action.capitalize(),
                                                               formatter.red('Failed')])))
                        if prefix is True:
                            output.append(formatter.add_line_prefix(command_return[0], (
                                command_return[1][1]) + "\n", True))
                        else:
                            output.append(command_return[1][1] + "\n")
                    else:
                        output.append(" ".join([command_return[0],
                                                action.capitalize(),
                                                formatter.red('Failed')]))
            except Exception, e:
                logging.critical(e.message)
                return "Error"
        return "\n".join(output)

    else:
        for server in host_list:
            if action == "check":
                ssh_return = ssh.return_remote_command_std_output(server,
                                                                  "/opt/NVMesh/%s*/services/nvmesh%s status" % (
                                                                      scope[0], scope))
            elif action == "start":
                ssh_return = ssh.return_remote_command_std_output(server,
                                                                  "/opt/NVMesh/%s*/services/nvmesh%s start" % (
                                                                      scope[0], scope))
            elif action == "stop":
                ssh_return = ssh.return_remote_command_std_output(server,
                                                                  "/opt/NVMesh/%s*/services/nvmesh%s stop" % (
                                                                      scope[0], scope))
            elif action == "restart":
                ssh_return = ssh.return_remote_command_std_output(server,
                                                                  "/opt/NVMesh/%s*/services/nvmesh%s restart" % (
                                                                      scope[0], scope))
            if ssh_return:
                if ssh_return[0] == 0:
                    if details is True:
                        output.append(' '.join([formatter.bold(server),
                                                action.capitalize(),
                                                formatter.green('OK')]))
                        if prefix is True:
                            output.append(formatter.add_line_prefix(server,
                                                                    (ssh_return[1]), True))
                        else:
                            output.append((ssh_return[1] + "\n"))
                    else:
                        output.append(" ".join([server,
                                                action.capitalize(),
                                                formatter.green('OK')]))
                else:
                    cli_exit.error = True
                    if details is True:
                        output.append(' '.join([formatter.bold(server),
                                                action.capitalize(),
                                                formatter.red('Failed')]))
                        if prefix is True:
                            output.append(formatter.add_line_prefix(server,
                                                                    (ssh_return[1] if not None else None), True))
                        else:
                            output.append((ssh_return[1] if not None else None + "\n"))
                    else:
                        output.append(" ".join([server,
                                                action.capitalize(),
                                                formatter.red('Failed')]))
            else:
                cli_exit.error = True
                output.append(" ".join([server,
                                        action.capitalize(),
                                        formatter.red('Failed')]))
        return "\n".join(output)


def attach_detach_volumes(action, clients, volumes):
    try:
        process_pool = Pool(len(clients))
        parallel_execution_map = []
        command_return_list = []
        if action == 'attach':
            for client in clients:
                command_line = " ".join(['nvmesh_attach_volumes', " ".join(volumes)])
                parallel_execution_map.append([str(client), str(command_line)])
            command_return_list = process_pool.map(run_parallel_ssh_command,
                                                   parallel_execution_map)
            process_pool.close()
        elif action == 'detach':
            for client in clients:
                command_line = " ".join(['nvmesh_detach_volumes', " ".join(volumes)])
                parallel_execution_map.append([str(client), str(command_line)])
            command_return_list = process_pool.map(run_parallel_ssh_command,
                                                   parallel_execution_map)
            process_pool.close()
        output = []
        for command_return in command_return_list:
            if command_return[0] != 0:
                cli_exit.error = True
            output.append(formatter.add_line_prefix(command_return[0],
                                                    command_return[1][1],
                                                    False))
        return "\n".join(output)
    except Exception, e:
        print(formatter.red("Error: " + e.message))
        logging.critical(e.message)
        cli_exit.error = True
        cli_exit.validate_exit()


def manage_mcm(clients, action):
    ssh = SSHRemoteOperations()
    try:
        if clients is not None:
            client_list = clients
        else:
            client_list = get_client_list(False)
        for client in client_list:
            if action == "stop":
                ssh.execute_remote_command(client, "/opt/NVMesh/client-repo/management_cm/managementCMClient.py stop")
                print client, "\tStopped the MangaementCM services."
            elif action == "start":
                ssh.execute_remote_command(client, "/opt/NVMesh/client-repo/management_cm/managementCMClient.py start")
                print client, "\tStarted the MangaementCM services."
            elif action == "restart":
                ssh.execute_remote_command(client, "/opt/NVMesh/client-repo/management_cm/managementCMClient.py")
                print client, "\tRestarted the MangaementCM services."
    except Exception, e:
        print(formatter.red("Error: " + e.message))
        logging.critical(e.message)
        cli_exit.error = True
        cli_exit.validate_exit()


def manage_cluster(details, action, prefix):
    try:
        if action == "check":
            print("Checking the NVMesh managers ...")
            NvmeshShell().poutput(manage_nvmesh_service('mgr', details, None, action, prefix, True, None))
            print("Checking the NVMesh targets ...")
            NvmeshShell().poutput(manage_nvmesh_service('target', details, None, action, prefix, True, None))
            print("Checking the NVMesh clients ...")
            NvmeshShell().poutput(manage_nvmesh_service('client', details, None, action, prefix, True, None))
        elif action == "start":
            print ("Starting the NVMesh managers ...")
            NvmeshShell().poutput(manage_nvmesh_service('mgr', details, None, action, prefix, True, None))
            time.sleep(3)
            print ("Starting the NVMesh targets ...")
            NvmeshShell().poutput(manage_nvmesh_service('target', details, None, action, prefix, True, None))
            print ("Starting the NVMesh clients ...")
            NvmeshShell().poutput(manage_nvmesh_service('client', details, None, action, prefix, True, None))
        elif action == "stop":
            print ("Stopping the NVMesh clients ...")
            NvmeshShell().poutput(manage_nvmesh_service('client', details, None, action, prefix, True, None))
            print ("Stopping the NVMesh targets ...")
            NvmeshShell().poutput(manage_nvmesh_service('target', details, None, action, prefix, True, True))
            print ("Stopping the NVMesh managers ...")
            NvmeshShell().poutput(manage_nvmesh_service('mgr', details, None, action, prefix, True, None))
        elif action == "restart":
            print ("Stopping the NVMesh clients ...")
            NvmeshShell().poutput(manage_nvmesh_service('client', details, None, 'stop', prefix, True, None))
            print ("Stopping the NVMesh targets ...")
            NvmeshShell().poutput(manage_nvmesh_service('target', details, None, 'stop', prefix, True, True))
            print ("Restarting the NVMesh managers ...")
            NvmeshShell().poutput(manage_nvmesh_service('mgr', details, None, 'restart', prefix, True, None))
            time.sleep(3)
            print ("Starting the NVMesh targets ...")
            NvmeshShell().poutput(manage_nvmesh_service('target', details, None, 'start', prefix, True, None))
            print ("Starting the NVMesh clients ...")
            NvmeshShell().poutput(manage_nvmesh_service('client', details, None, 'start', prefix, True, None))
    except Exception, e:
        print(formatter.red("Error: " + e.message))
        logging.critical(e.message)
        cli_exit.error = True
        cli_exit.validate_exit()


def run_parallel_ssh_command(argument):
    ssh = SSHRemoteOperations()
    try:
        output = ssh.return_remote_command_std_output(argument[0], argument[1])
        if output:
            if output[0] != 0:
                cli_exit.error = True
            return argument[0], output
        else:
            cli_exit.error = True
            return argument[0], [1, "Failed on %s" % argument[0]]
    except Exception, e:
        print(formatter.red("Error: " + e.message))
        logging.critical(e.message)
        cli_exit.error = True
        cli_exit.validate_exit()


def manage_volume(action, name, capacity, description, disk_classes, server_classes, limit_by_nodes, limit_by_disks,
                  awareness, raid_level, stripe_width, vpg, force, ec_parity, ec_node_redundancy):
    if get_api_ready() == 0:
        api_payload = {}
        payload = {}
        if action == "create":
            payload = {
                "name": name,
                "capacity": "MAX" if str(capacity[0]).upper() == "MAX" else int(humanfriendly.parse_size(capacity[0],
                                                                                                         binary=True)),
            }
            if description is not None:
                payload["description"] = description[0]
            if disk_classes is not None:
                payload["diskClasses"] = disk_classes
            if server_classes is not None:
                payload["serverClasses"] = server_classes
            else:
                payload["serverClasses"] = []
            if limit_by_nodes is not None:
                payload["limitByNodes"] = limit_by_nodes
            else:
                payload["limitByNodes"] = []
            if limit_by_disks is not None:
                payload["limitByDisks"] = limit_by_disks
            else:
                payload["limitByDisks"] = []
            if awareness is not None:
                payload["domain"] = awareness[0]
            if raid_level is not None and vpg is None:
                payload["RAIDLevel"] = RAID_LEVELS[raid_level[0]]
                if raid_level[0] == "lvm":
                    pass
                if raid_level[0] == "con":
                    pass
                elif raid_level[0] == "0":
                    payload["stripeSize"] = 32
                    payload["stripeWidth"] = int(stripe_width[0])
                elif raid_level[0] == "1":
                    payload["numberOfMirrors"] = 1
                elif raid_level[0] == "10":
                    payload["stripeSize"] = 32
                    payload["stripeWidth"] = int(stripe_width[0])
                    payload["numberOfMirrors"] = 1
                elif raid_level[0] == "ec":
                    payload['protectionLevel'] = PROTECTION_LEVELS[int(ec_node_redundancy[0])]
                    payload['dataBlocks'] = int(ec_parity[0].split('+')[0])
                    payload['parityBlocks'] = int(ec_parity[0].split('+')[1])
                    payload["stripeSize"] = 32
                    payload["stripeWidth"] = 1
            elif vpg is not None and raid_level is None:
                payload["VPG"] = vpg[0]
            api_payload["create"] = [payload]
            api_payload["remove"] = []
            api_payload["edit"] = []
            api_return = json.loads(nvmesh.manage_volume(api_payload))
            if api_return['create'][0]['success'] is True:
                return " ".join(["Volume",
                                 name,
                                 "successfully created.",
                                 formatter.green('OK')])

            else:
                cli_exit.error = True
                return " ".join(["Couldn't create volume", name,
                                 formatter.red('Failed')])

        elif action == 'remove':
            api_return = []
            output = []
            for volume in name:
                payload["_id"] = volume
                if force:
                    payload["force"] = True
                api_payload["remove"] = [payload]
                api_payload["create"] = []
                api_payload["edit"] = []
                api_return.append(json.loads(nvmesh.manage_volume(api_payload)))
            for item in api_return:
                if item['remove'][0]['success'] is True:
                    output.append(" ".join(["Volume",
                                            item['remove'][0]['id'],
                                            "successfully deleted.",
                                            formatter.green('OK')]))
                else:
                    output.append(" ".join([formatter.red('Failed'),
                                            "to delete", item['remove'][0]['id'],
                                            "-",
                                            item['remove'][0]['ex']]))
                    cli_exit.error = True
            return "\n".join(output)


def manage_vpg(action, name, capacity, description, disk_classes, server_classes, awareness, raid_level, stripe_width):
    if get_api_ready() == 0:
        api_payload = {}
        payload = {}
        if action == "save":
            payload = {
                "name": name,
                "capacity": "MAX" if str(capacity[0]).upper() == "MAX" else int(humanfriendly.parse_size(capacity[0],
                                                                                                         binary=True)),
            }
            if description is not None:
                payload["description"] = description[0]
            if disk_classes is not None:
                payload["diskClasses"] = disk_classes
            if server_classes is not None:
                payload["serverClasses"] = server_classes
            else:
                payload["serverClasses"] = []
            if awareness is not None:
                payload["domain"] = awareness[0]
            payload["RAIDLevel"] = RAID_LEVELS[raid_level[0]]
            payload["stripeWidth"] = stripe_width
            payload["stripeSize"] = 32
            payload["numberOfMirrors"] = 1
            api_return = json.loads(nvmesh.manage_vpg("save", api_payload))
            if api_return['create'][0]['success'] is True:
                return " ".join(["VPG",
                                 name,
                                 "successfully created.",
                                 formatter.green('OK')])
            else:
                cli_exit.error = True
                return " ".join(["Couldn't create vpg", name, formatter.red('Failed')])

        elif action == 'remove':
            api_return = []
            payload["_id"] = name
            api_return.append(json.loads(nvmesh.manage_volume("delete", api_payload)))
            if api_return['remove'][0]['success'] is True:
                return " ".join(["Volume", name, "successfully deleted.", formatter.green('OK')])
            else:
                cli_exit.error = True
                return " ".join([formatter.red('Failed'), "to delete", name])


def update_volume(volume, capacity, description, drives, targets, drive_classes, target_classes):
    if capacity:
        volume["capacity"] = "MAX" if str(capacity[0]).upper() == "MAX" else int(
            humanfriendly.parse_size(capacity[0], binary=True))
    if description:
        volume["description"] = " ".join(description)
    if drives:
        volume["limitByDisks"] = drives
    if targets:
        volume["limitByNodes"] = targets
    if target_classes:
        volume["serverClasses"] = target_classes
    if drive_classes:
        volume["diskClasses"] = drive_classes
    api_payload = dict()
    api_payload["remove"] = []
    api_payload["create"] = []
    api_payload["edit"] = [volume]
    api_return = json.loads(nvmesh.manage_volume(api_payload))
    if api_return["edit"][0]["success"] is True:
        output = " ".join(["Volume",
                           volume["name"],
                           "successfully updated.",
                           formatter.green('OK')])
        return output
    else:
        output = " ".join([formatter.red('Failed'),
                           "to update",
                           volume["name"]])
        cli_exit.error = True
        return output


def update_target_class(target_class, servers, description):
    if servers:
        target_class["targetNodes"] = servers
    if description:
        target_class["description"] = " ".join(description)
    api_payload = [target_class]
    api_return = json.loads(nvmesh.update_target_class(api_payload))
    print api_return
    if api_return[0]["success"] is True:
        output = " ".join(["target class",
                           target_class["name"],
                           "successfully updated.",
                           formatter.green('OK')])
        return output
    else:
        output = " ".join([formatter.red('Failed'),
                           "to update",
                           target_class["name"],
                           "-",
                           api_return[0]["err"]])
        cli_exit.error = True
        return output


def update_drive_class(drive_class, drives, description, file_path):
    if description:
        drive_class["description"] = description[0]
    if file_path:
        drive_class["disks"][0]["disks"] = parse_drive_args(open(file_path[0], 'r').readlines())
    if drives:
        drive_class["disks"][0]["disks"] = parse_drive_args(drives)
    api_payload = [drive_class]
    api_return = json.loads(nvmesh.update_drive_class(api_payload))
    if api_return[0]["success"] is True:
        output = " ".join(["Drive class",
                           drive_class["_id"],
                           "successfully updated.",
                           formatter.green('OK')])
        return output
    else:
        output = " ".join([formatter.red('Failed'),
                           "to update",
                           drive_class["_id"]])
        cli_exit.error = True
        return output


def manage_drive(action, drive, format_type):
    get_api_ready()
    if action == 'evict':
        payload = {}
        payload["Ids"] = drive
        api_payload = payload
        api_return = json.loads(nvmesh.evict_drive(api_payload))
        if api_return:
            output_list = []
            for line in api_return:
                if line['success']:
                    output = " ".join(["Drive", line['id'], "successfully evicted.", formatter.green("OK")])
                    output_list.append(output)
                else:
                    cli_exit.error = True
                    output = " ".join(["Drive", line['_id'], "not evicted!", line['err'],
                                       formatter.red("Failed")])
                    output_list.append(output)
            return "\n".join(output_list)
        else:
            cli_exit.error = True
            output = " ".join(["Drive", drive, "not evicted!", formatter.red("Failed")])
            return output
    elif action == 'delete':
        payload = {}
        payload["Ids"] = drive
        api_payload = payload
        api_return = json.loads(nvmesh.delete_drive(api_payload))
        if api_return:
            output_list = []
            for line in api_return:
                if line['success']:
                    output = " ".join(["Drive", line['id'], "successfully deleted.", formatter.green("OK")])
                    output_list.append(output)
                else:
                    cli_exit.error = True
                    output = " ".join(["Drive", line['id'], "not deleted!", line['err'],
                                       formatter.red("Failed")])
                    output_list.append(output)
            return "\n".join(output_list)
        else:
            cli_exit.error = True
            output = " ".join(["Drive", drive, "not deleted!", formatter.red("Failed")])
            return output
    elif action == 'format':
        payload = {}
        payload['formatType'] = FORMAT_TYPES[format_type]
        payload['diskIDs'] = drive
        api_return = json.loads(nvmesh.format_drive(payload))
        if api_return:
            output_list = []
            for line in api_return:
                if line['success']:
                    output = " ".join(["Drive", line['_id'], "successfully formatted.", formatter.green("OK")])
                    output_list.append(output)
                else:
                    cli_exit.error = True
                    output = " ".join(["Drive", line['_id'], "not formatted!", line['error'],
                                       formatter.red("Failed")])
                    output_list.append(output)
            return "\n".join(output_list)
        else:
            cli_exit.error = True
            output = " ".join(["Drive/s", " ".join(drive), "not formatted!", formatter.red("Failed")])
            return output


def manage_nic(action, nic_id):
    if get_api_ready() == 0:
        api_return = []
        output = []
        payload = {}
        if action == "delete":
            payload["nicID"] = nic_id.strip()

            api_return = json.loads(nvmesh.delete_nic(payload))
            if api_return:
                if api_return['success']:
                    output = " ".join(["NIC", api_return['_id'], "successfully deleted.", formatter.green("OK")])
                else:
                    cli_exit.error = True
                    output = " ".join(["NIC", api_return['_id'], "not deleted!", api_return['error'],
                                       formatter.red("Failed")])
                return output
            else:
                cli_exit.error = True
                output = " ".join(["NIC", " ".join(nic_id.strip()), "not formatted!", formatter.red("Failed")])
                return output


def manage_drive_class(action, class_list, drives, model, name, description, domains, file_path):
    if get_api_ready() == 0:
        api_return = []
        output = []
        payload = {}
        if action == "autocreate":
            model_list = get_drive_models(pretty=False)
            for model in model_list:
                drives = json.loads(nvmesh.get_disk_by_model(model[0]))
                drive_list = []
                for drive in drives:
                    drive_list.append(
                        {
                            "diskID": drive["disks"]["diskID"],
                            "node_id": drive["node_id"]
                        }
                    )
                payload["_id"] = re.sub("(?<=_)_|_(?=_)", "", model[0])
                payload["description"] = "automatically created"
                payload["disks"] = [{"model": model[0],
                                     "disks": drive_list}]
                api_payload = [payload]
                api_return.append([re.sub("(?<=_)_|_(?=_)",
                                          "",
                                          model[0]),
                                   json.dumps(nvmesh.manage_drive_class("save",
                                                                        api_payload))])
            for line in api_return:
                if "null" in line[1]:
                    output.append(" ".join(["Drive Class",
                                            line[0],
                                            "successfully created.",
                                            formatter.green('OK')]))
                else:
                    output.append(" ".join([formatter.red('Failed'),
                                            "\t",
                                            "Couldn't create Drive Class",
                                            line[0],
                                            " - ",
                                            "Check for duplicates."]))
                    cli_exit.error = True
            return "\n".join(output)
        elif action == "save":
            payload["_id"] = name[0]
            if description:
                payload["description"] = description
            if domains:
                payload["domains"] = parse_domain_args(domains)
            if file_path:
                payload["disks"] = [{"model": model[0],
                                     "disks": parse_drive_args(open(file_path[0], 'r').readlines())}]
            else:
                payload["disks"] = [{"model": model[0],
                                     "disks": parse_drive_args(drives)}]
            api_payload = [payload]
            api_return.append([name[0],
                               json.dumps(nvmesh.manage_drive_class("save", api_payload))])
            for line in api_return:
                if "null" in line[1]:
                    output.append(" ".join(["Drive Class",
                                            name[0],
                                            "successfully created.",
                                            formatter.green('OK')]))
                else:
                    output.append(" ".join([formatter.red('Failed'),
                                            "\t", "Couldn't create Drive Class",
                                            name[0],
                                            " - ",
                                            "Check for duplicates."]))
                    cli_exit.error = True
            return "\n".join(output)

        elif action == "delete":
            for drive_class in class_list:
                payload = [{"_id": drive_class}]
                return_info = json.loads(nvmesh.manage_drive_class("delete", payload))

                if return_info[0]["success"] is True:
                    output.append(
                        " ".join(["Drive Class",
                                  drive_class,
                                  "successfully deleted.",
                                  formatter.green('OK')]))
                else:
                    output.append(" ".join([formatter.red('Failed'),
                                            "\t", "Couldn't delete Drive Class.",
                                            drive_class,
                                            " - ",
                                            return_info[0]["msg"]]))
                    cli_exit.error = True
            return "\n".join(output)


def manage_target_class(action, class_list, name, servers, description, domains):
    if get_api_ready() == 0:
        api_return = []
        output = []
        payload = {}
        if action == "autocreate":
            for target in get_target_list(short=False):
                payload["name"] = target.split(".")[0]
                payload["targetNodes"] = [target]
                payload["description"] = "automatically created"
                api_payload = [payload]
                api_return.append([target.split(".")[0],
                                   json.dumps(nvmesh.manage_target_class("save", api_payload))])
            for line in api_return:
                if "null" in line[1]:
                    output.append(" ".join(["Target Class",
                                            line[0],
                                            "successfully created.",
                                            formatter.green('OK')]))
                else:
                    output.append(" ".join([formatter.red('Failed'),
                                            "\t", "Couldn't create Target Class",
                                            line[0],
                                            " - ",
                                            "Check for duplicates."]))
                    cli_exit.error = True
            return "\n".join(output)
        elif action == "delete":
            for target_class in class_list:
                payload = [{"_id": target_class}]
                return_info = json.loads(nvmesh.manage_target_class("delete", payload))

                if return_info[0]["success"] is True:
                    output.append(
                        " ".join(["Target Class",
                                  target_class,
                                  "successfully deleted.",
                                  formatter.green('OK')]))
                else:
                    output.append(" ".join([formatter.red('Failed'),
                                            "\t",
                                            "Couldn't delete Target Class",
                                            target_class,
                                            " - ",
                                            return_info[0]["msg"]]))
                    cli_exit.error = True
            return "\n".join(output)
        elif action == "save":
            payload["name"] = name
            if description is not None:
                payload["description"] = description
            payload["targetNodes"] = servers
            if domains is not None:
                payload["domains"] = domains
            api_payload = [payload]
            api_return.append([name, json.dumps(nvmesh.manage_target_class("save", api_payload))])
            for line in api_return:
                if "null" in line[1]:
                    output.append(" ".join(["Target Class",
                                            line[0],
                                            "successfully created.",
                                            formatter.green('OK')]))
                else:
                    output.append(" ".join([formatter.red('Failed'),
                                            "\t", "Couldn't create Target Class",
                                            line[0],
                                            " - ",
                                            "Check for duplicates."]))
                    cli_exit.error = True
            return "\n".join(output)


def show_drives(details, targets, tsv):
    if get_api_ready() == 0:
        drive_list = []
        target_list = get_target_list(short=False)
        for target in target_list:
            if targets is not None and target.split('.')[0] not in targets:
                continue
            else:
                target_details = json.loads(nvmesh.get_server_by_id(target))
                for disk in target_details['disks']:
                    if disk['isExcluded']:
                        pass
                    else:
                        vendor = disk['Vendor'] if not str(disk['Vendor']).lower() in NVME_VENDORS else \
                            NVME_VENDORS[str(disk['Vendor']).lower()]
                        if disk["status"].lower() == "ok":
                            status = u'\u2705'
                        elif disk["status"].lower() == "not_initialized":
                            status = formatter.yellow("Not Initialized")
                            drive_format = "n/a"
                        elif disk["status"].lower() == "initializing":
                            status = "Initializing - %s%%" % (disk['nZeroedBlks'] * 100 / disk['availableBlocks'])
                        else:
                            status = u'\u274C'
                        if 'metadata_size' in disk:
                            if int(disk['metadata_size']) == 8:
                                ec_support = "Yes"
                            else:
                                ec_support = "No"
                        else:
                            ec_support = "n/a"
                        if 'metadata_size' in disk:
                            if disk["status"].lower() != "not_initialized":
                                if disk['metadata_size'] > 0:
                                    drive_format = "EC"
                                else:
                                    drive_format = "Legacy"
                        else:
                            drive_format = "n/a"
                        if 'isOutOfService' in disk:
                            in_service = formatter.red("No")
                            status = "n/a"
                        else:
                            in_service = formatter.green("Yes")
                        if details:
                            drive_list.append([vendor,
                                               disk['Model'],
                                               disk['diskID'],
                                               humanfriendly.format_size((disk['block_size'] * disk['blocks']),
                                                                         binary=True),
                                               status,
                                               in_service,
                                               ec_support,
                                               drive_format,
                                               humanfriendly.format_size(disk['block_size'], binary=True),
                                               " ".join([str(100 - int((disk['Available_Spare'].split("_")[0]))), "%"]),
                                               target,
                                               disk['Numa_Node'],
                                               disk['Submission_Queues']])
                        else:
                            drive_list.append([vendor,
                                               re.sub("(?<=_)_|_(?=_)", "", disk['Model']),
                                               disk['diskID'],
                                               humanfriendly.format_size((disk['block_size'] * disk['blocks']),
                                                                         binary=True),
                                               status,
                                               in_service,
                                               ec_support,
                                               drive_format,
                                               target])
        if tsv:
            return formatter.print_tsv(drive_list)
        if details:
            return format_smart_table(sorted(drive_list),
                                      ['Vendor',
                                       'Model',
                                       'Drive ID',
                                       'Size',
                                       'Status',
                                       'In Service',
                                       'EC Support',
                                       'Format',
                                       'Sector Size',
                                       'Wear',
                                       'Target',
                                       'Numa',
                                       'QPs'])
        else:
            return format_smart_table(sorted(drive_list), ['Vendor',
                                                           'Model',
                                                           'Drive ID',
                                                           'Size',
                                                           'Status',
                                                           'In Service',
                                                           'EC Support',
                                                           'Format',
                                                           'Target'])


def show_drive_models(details):
    if not details:
        return format_smart_table(get_drive_models(pretty=True), ["Drive Model", "Drives"])
    else:
        return format_smart_table(get_drive_models(pretty=False), ["Drive Model", "Drives"])


def get_drive_models(pretty):
    if get_api_ready() == 0:
        model_list = []
        json_drive_models = json.loads(nvmesh.get_disk_models())
        for model in json_drive_models:
            if pretty:
                model_list.append([re.sub("(?<=_)_|_(?=_)", "", model["_id"]), model["available"]])
            else:
                model_list.append([model["_id"], model["available"]])
        return model_list


def start_shell():
    reload(sys)
    sys.setdefaultencoding('utf-8')
    history_file = os.path.expanduser('~/.nvmesh_shell_history')
    if not os.path.exists(history_file):
        with open(history_file, "w") as history:
            history.write("")
    readline.read_history_file(history_file)
    atexit.register(readline.write_history_file, history_file)
    shell = NvmeshShell()
    if os.path.exists(os.path.expanduser('~/.nvmesh_cli_ack')):
        pass
    else:
        print("Before using this software, please read and acknowledge the licensing terms and agreement.")
        if 'y' in raw_input("Do you want to proceed? [Yes/No]: ").lower():
            NvmeshShell().ppaged(__license__)
            if 'y' in raw_input("\nDo you agree to the licensing terms? [Yes/No]: ").lower():
                ack = open(os.path.expanduser('~/.nvmesh_cli_ack'), 'w')
                ack.write(str(""))
                ack.close()
            else:
                print("Good bye.")
                exit(0)
        else:
            print("Good bye.")
            exit(0)

    if len(sys.argv) > 1:
        cli_exit.is_interactive = False
        shell.onecmd(' '.join(sys.argv[1:]))
    else:
        cli_exit.is_interactive = True
        shell.cmdloop('''
Copyright (c) 2018 Excelero, Inc. All rights reserved.

This program comes with ABSOLUTELY NO WARRANTY; for licensing and warranty details type 'show license'.
This is free software, and you are welcome to redistribute it under certain conditions; type ' show license' for 
details.

Starting the NVMesh CLI - version 1.3 - build %s ...''' % __version__)


if __name__ == '__main__':
    start_shell()
