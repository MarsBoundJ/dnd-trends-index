/*************************************************************************
* ADOBE CONFIDENTIAL
* ___________________
*
*  Copyright 2015 Adobe Systems Incorporated
*  All Rights Reserved.
*
* NOTICE:  All information contained herein is, and remains
* the property of Adobe Systems Incorporated and its suppliers,
* if any.  The intellectual and technical concepts contained
* herein are proprietary to Adobe Systems Incorporated and its
* suppliers and are protected by all applicable intellectual property laws,
* including trade secret and or copyright laws.
* Dissemination of this information or reproduction of this material
* is strictly forbidden unless prior written permission is obtained
* from Adobe Systems Incorporated.
**************************************************************************/
export class ConfigLoader{constructor(){this.CONFIG_PATH=chrome.runtime.getURL("dist/preferences-config.json"),this.fetchPromise=null}async init(){}async loadConfiguration(){return this.fetchPromise||(this.fetchPromise=(async()=>{try{const t=await fetch(this.CONFIG_PATH);if(!t.ok)throw new Error(`Failed to load config: ${t.status} ${t.statusText}`);const r=await t.json();if(!r||!r.tabs)throw new Error("Invalid config structure: missing 'tabs' property");return r}catch(t){throw console.error("Error loading preferences config:",t),t}finally{this.fetchPromise=null}})()),this.fetchPromise}}