/*
 * Created on Fri Mar 03 2023
 *
 * Copyright 2021 YugaByte, Inc. and Contributors
 * Licensed under the Polyform Free Trial License 1.0.0 (the "License")
 * You may not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://github.com/YugaByte/yugabyte-db/blob/master/licenses/POLYFORM-FREE-TRIAL-LICENSE-1.0.0.txt
 */

import React, { useCallback, useState } from 'react';

import Prism, { Token } from 'prismjs';
import 'prismjs/components/prism-json';
import clsx from 'clsx';
import { isEmpty } from 'lodash';
import { BasePoint, Editor, NodeEntry, Range, Selection, Transforms } from 'slate';
import { ReactEditor } from 'slate-react';
import { IYBSlatePlugin, SlateRenderElementProps, SlateRenderLeafProps } from '../IPlugin';
import { ALERT_VARIABLE_REGEX, Portal, deleteNChars, getBeforeNChars, isBlockActive, nonActivePluginReturnType } from '../PluginUtils';

import { CustomText } from '../custom-types';
import { AddAlertVariablesPopup } from '../../../../features/alerts/TemplateComposer/AlertVariablesPopup';
import { ALERT_VARIABLE_END_TAG, ALERT_VARIABLE_START_TAG } from '../alert/AlertVariablesPlugin';
import { CustomVariable, SystemVariables } from '../../../../features/alerts/TemplateComposer/ICustomVariables';

/**
 * styles for syntax highlighting
 */
import './JSONStyles.scss';

const PLUGIN_NAME = 'JSON';
export const JSON_BLOCK_TYPE = 'jsonCode';

// default indendation is 4 spaces;
const Indendation = ' '.repeat(4);


const PRISM_VARIABLE_PATTERN = {
    variable: {
        pattern: ALERT_VARIABLE_REGEX
    }
}

// extend Prism's JSON vocabulary
Prism.languages.insertBefore('json', 'string', {
    string: {
        inside: PRISM_VARIABLE_PATTERN,
        ...Prism.languages['json']['string']
    } as any,
    ...PRISM_VARIABLE_PATTERN
})

const AUTO_COMPLETE_TAGS = {
    "'": "'",
    '"': '"',
    "{": "}",
    "[": "]"
}


export const useJSONPlugin: IYBSlatePlugin = ({ enabled, editor }) => {

    const [position, setPosition] = useState<DOMRect | null>(null);


    if (!enabled) {
        return { name: PLUGIN_NAME, ...nonActivePluginReturnType };
    }

    const hidePopver = () => {
        setPosition(null);
        // restore cursor position in editor
        const sel = editor.selection;
        Transforms.deselect(editor);
        if (sel) {
            ReactEditor.focus(editor);
            Transforms.select(editor, sel);
        }
    };


    const insertVariable = (variable: CustomVariable | SystemVariables) => {
        hidePopver();

        //if preceding 2 chars is '{{', delete it
        if (getBeforeNChars(editor, ALERT_VARIABLE_START_TAG.length) === ALERT_VARIABLE_START_TAG) {
            deleteNChars(editor, ALERT_VARIABLE_START_TAG.length, true);
        }

        editor.insertText(`${ALERT_VARIABLE_START_TAG}${variable.name}${ALERT_VARIABLE_END_TAG}`)

    }

    editor['insertJSONVariable'] = insertVariable;



    const renderElement = ({ attributes, children, element }: SlateRenderElementProps) => {
        if (element.type === JSON_BLOCK_TYPE) {
            return <p {...attributes} className='jsonCode'>{children}</p>
        }
        return undefined;
    };

    const renderLeaf = ({ attributes, children, leaf }: SlateRenderLeafProps) => {
        if (leaf.decoration?.JSON.type) {
            return <span {...attributes} className={clsx('jsonStyles', leaf.decoration?.JSON.type)}>{children}</span>
        }
        return undefined;
    };

    const onKeyDown = (e: React.KeyboardEvent<HTMLDivElement>) => {


        if (Boolean(position)) {
            hidePopver();
            if (getBeforeNChars(editor, ALERT_VARIABLE_START_TAG.length) === ALERT_VARIABLE_START_TAG) {
                deleteNChars(editor, ALERT_VARIABLE_START_TAG.length, true);
            }
            return true;
        }

        const { selection } = editor;
        const beforeChar = getBeforeNChars(editor, 1);

        //if last 2 characters is '{{', then open the popover
        if (selection && beforeChar && beforeChar + e.key === ALERT_VARIABLE_START_TAG) {
            const domRange = ReactEditor.toDOMRange(editor, selection);
            setPosition(domRange.getBoundingClientRect());
            //return true, because we handled it
            return true;
        }

        // Below are editor enhancement fixes.


        // if Tab key is pressed, instead of shifting focus, insert the default indendation
        if (e.key === 'Tab') {
            e.preventDefault();
            editor.insertText(Indendation);
            return true;
        }

        // if backspace is pressed and the entire line is empty, delete all the line
        if (e.key === 'Backspace' && selection && Range.isCollapsed(selection)) {
            const currentLine = Editor.string(editor, selection.anchor.path)
            if (currentLine.trim().length === 0) {
                Transforms.delete(editor, { at: selection, distance: 1, unit: 'line', reverse: true })
                e.preventDefault();
                return true;
            }

        }

        //if enter is pressed, go to next line and insert the line indendation

        if (e.key === 'Enter' && selection) {

            e.preventDefault();
            // calculate current line indendation
            const currentLine = Editor.string(editor, selection.anchor.path)
            const lineIndentation = currentLine.match(/\s*/g)?.[0] ?? '';
            editor.insertBreak();
            editor.insertText(lineIndentation);
            return true;
        }

        // if there is bracket present, add a extra indent to it.
        if (Object.keys(AUTO_COMPLETE_TAGS).includes(e.key) && editor.selection) {

            let [_, end] = Range.edges(editor.selection);

            const after = Editor.after(editor, end, {
                unit: 'word',
            });

            const wordAfter =
                after && Editor.string(editor, { anchor: end, focus: after });
            if (!wordAfter || wordAfter?.trim() === '') {
                e.preventDefault()
                Transforms.insertText(editor, e.key + AUTO_COMPLETE_TAGS[e.key]);

                Transforms.move(editor, {
                    distance: AUTO_COMPLETE_TAGS[e.key].length,
                    unit: 'character',
                    reverse: true
                })
                return true;
            }
        }
        return false;
    };

    return {
        name: PLUGIN_NAME,
        renderElement,
        onKeyDown,
        isEnabled: () => enabled,
        renderLeaf,
        decorator,
        defaultComponents: [
            useCallback(() => {
                return (
                    <Portal>
                        {position ? (
                            <div
                                style={{
                                    top: `${position.top + window.pageYOffset + 20}px`,
                                    left: `${position.left + window.pageXOffset}px`,
                                    position: 'absolute',
                                    zIndex: '999'
                                }}
                            >
                                <AddAlertVariablesPopup
                                    show={Boolean(position)}
                                    onCustomVariableSelect={insertVariable}
                                    onSystemVariableSelect={insertVariable}
                                />
                            </div>
                        ) : null}
                    </Portal>
                );
            }, [position])
        ]
    };
};

type JSONSelection = Selection & { decoration: { JSON: { type: string } } };

// Extend Prism's tokenise function for syntax highlighting
const decorator = (entry: NodeEntry<CustomText>): JSONSelection[] => {
    const [node, path] = entry;

    const nodeText = node.text;

    if (!nodeText) return [];

    const tokens = Prism.tokenize(nodeText, Prism.languages['json']);

    const decorators: JSONSelection[] = [];

    let offset = 0;

    tokens.forEach((token) => {
        if (typeof token === 'string') {
            offset = offset + token.length;
            return;
        };

        if (token.type === 'string') {
            if (Array.isArray(token.content)) {
                token.content.forEach((t) => {
                    decorators.push(getDecorator(path, offset, t));
                    offset += t.length;
                })
            }
            return
        }
        decorators.push(getDecorator(path, offset, token));
        offset += token.length

    })
    return decorators.filter((x) => !isEmpty(x));
}

const getDecorator = (path: BasePoint['path'], offset: BasePoint['offset'], token: Token | string) => {
    return {
        anchor: {
            path,
            offset
        },
        focus: {
            path,
            offset: offset + token.length
        },
        decoration: {
            JSON: {
                type: token instanceof Token ? token.type : 'string'
            }
        }
    }
}
